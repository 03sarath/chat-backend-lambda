import boto3
from datetime import datetime, timezone

dynamodb = boto3.client('dynamodb')
TABLE_CONNECTIONS = 'Chat_connections'
TABLE_CHAT_INFO = 'chat_information'
GSI_NAME = 'guest_id-session_id-index'

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    print(f"Processing disconnect for connection_id: {connection_id}")

    try:
        # Fetch connection item to extract guest_id, session_id, and event_id
        resp = dynamodb.get_item(
            TableName=TABLE_CONNECTIONS,
            Key={'connection_id': {'S': connection_id}}
        )

        if 'Item' not in resp:
            print(f"No connection found for connection_id: {connection_id}")
            return {'statusCode': 404, 'body': 'Connection not found'}

        item = resp['Item']
        guest_id = item.get('guest_id', {}).get('S')
        session_id = item.get('session_id', {}).get('S')
        event_id = item.get('event_id', {}).get('S')
        is_host = item.get('is_host', {}).get('BOOL', False)

        print(f"Found connection details - guest_id: {guest_id}, session_id: {session_id}, event_id: {event_id}, is_host: {is_host}")

        # First delete the specific connection
        try:
            dynamodb.delete_item(
                TableName=TABLE_CONNECTIONS,
                Key={'connection_id': {'S': connection_id}}
            )
            print(f"Successfully deleted connection: {connection_id}")
        except Exception as e:
            print(f"Error deleting specific connection: {str(e)}")
            raise e

        # Then delete any other connections for the same guest_id, session_id, and event_id
        try:
            existing_conns = dynamodb.query(
                TableName=TABLE_CONNECTIONS,
                IndexName=GSI_NAME,
                KeyConditionExpression='guest_id = :g AND session_id = :s',
                ExpressionAttributeValues={
                    ':g': {'S': guest_id},
                    ':s': {'S': session_id}
                }
            ).get('Items', [])

            print(f"Found {len(existing_conns)} existing connections to check")

            for conn in existing_conns:
                if conn.get('event_id', {}).get('S') == event_id:
                    conn_id = conn['connection_id']['S']
                    if conn_id != connection_id:  # Skip the one we already deleted
                        try:
                            dynamodb.delete_item(
                                TableName=TABLE_CONNECTIONS,
                                Key={'connection_id': {'S': conn_id}}
                            )
                            print(f"Successfully deleted additional connection: {conn_id}")
                        except Exception as e:
                            print(f"Error deleting additional connection {conn_id}: {str(e)}")
        except Exception as e:
            print(f"Error querying or deleting additional connections: {str(e)}")
            raise e

        # If this was the host disconnecting, update the chat information
        if is_host:
            try:
                dynamodb.update_item(
                    TableName=TABLE_CHAT_INFO,
                    Key={
                        'session_id': {'S': session_id},
                        'event_id': {'S': event_id}
                    },
                    UpdateExpression='REMOVE connection_id',
                    ConditionExpression='attribute_exists(session_id) AND attribute_exists(event_id)'
                )
                print(f"Successfully updated chat info for host disconnect")
            except dynamodb.exceptions.ConditionalCheckFailedException:
                print("Chat info record doesn't exist, which is fine")
            except Exception as e:
                print(f"Error updating chat info: {str(e)}")

        return {'statusCode': 200, 'body': 'All related connections removed'}

    except Exception as e:
        print(f"Disconnect error: {str(e)}")
        return {'statusCode': 500, 'body': 'Internal server error'}
