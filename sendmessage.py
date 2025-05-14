import boto3
import json
from datetime import datetime, timezone

dynamodb = boto3.client('dynamodb')
TABLE_CHAT_INFO = 'chat_information'
TABLE_CONNECTIONS = 'Chat_connections'
API_GATEWAY_ENDPOINT = 'https://0ug96h4n9g.execute-api.us-east-1.amazonaws.com/production/'

def lambda_handler(event, context):
    try:
        connection_id = event['requestContext']['connectionId']
        body = json.loads(event.get('body', '{}'))
        message = body.get('message')
        to = body.get('to')

        if not message or not to:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Missing message or recipient'})
            }

        # Lookup sender's connection info
        sender_conn = dynamodb.get_item(
            TableName=TABLE_CONNECTIONS,
            Key={'connection_id': {'S': connection_id}}
        ).get('Item')

        if not sender_conn:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Sender not connected'})
            }

        sender_id = sender_conn['guest_id']['S']
        session_id = sender_conn['session_id']['S']
        event_id = sender_conn['event_id']['S']
        is_host = sender_conn.get('is_host', {}).get('BOOL', False)
        timestamp = datetime.now(timezone.utc).isoformat()

        # Query for recipient in same session
        receiver_conn = dynamodb.query(
            TableName=TABLE_CONNECTIONS,
            IndexName='guest_id-session_id-index',
            KeyConditionExpression='guest_id = :g AND session_id = :s',
            ExpressionAttributeValues={
                ':g': {'S': to},
                ':s': {'S': session_id}
            }
        )

        if receiver_conn['Count'] == 0:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'Recipient not connected in same session'})
            }

        receiver_conn_id = receiver_conn['Items'][0]['connection_id']['S']

        # Update chat history in Chat_information using sender's session_id + event_id
        dynamodb.update_item(
            TableName=TABLE_CHAT_INFO,
            Key={
                'session_id': {'S': session_id},
                'event_id': {'S': event_id}
            },
            UpdateExpression="SET chat_history = list_append(if_not_exists(chat_history, :empty), :msg)",
            ExpressionAttributeValues={
                ':msg': {'L': [{
                    'M': {
                        'Timestamp': {'S': timestamp},
                        'Sender': {'S': sender_id},
                        'Message': {'S': message} 
                    }
                }]},
                ':empty': {'L': []}
            }
        )

        # Send message to recipient
        apig = boto3.client('apigatewaymanagementapi', endpoint_url=API_GATEWAY_ENDPOINT)
        try:
            apig.post_to_connection(
                ConnectionId=receiver_conn_id,
                Data=json.dumps({
                    'from': sender_id,
                    'message': message,
                    'timestamp': timestamp,
                    'session_id': session_id,
                    'is_host': is_host
                }).encode('utf-8')
            )
        except apig.exceptions.GoneException:
            # Recipient disconnected
            dynamodb.delete_item(
                TableName=TABLE_CONNECTIONS,
                Key={'connection_id': {'S': receiver_conn_id}}
            )
            return {
                'statusCode': 410,
                'body': json.dumps({'error': 'Recipient disconnected'})
            }

        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'Message sent'})
        }

    except Exception as e:
        print(f"Send Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Server error: {str(e)}"})
        }
