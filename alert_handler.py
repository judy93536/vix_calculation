# alert_handler.py
import os
import base64
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import argparse

# Scopes required for sending Gmail emails
SCOPES = ['https://www.googleapis.com/auth/gmail.send']

# Path to your credentials and token files
CREDENTIALS_FILE = '/home/judy/client_secret_838163655325-v3hqjdf655aqae8pgt1n2kl72o4bmef4.apps.googleusercontent.com.json'
TOKEN_FILE = '/home/judy/.gmail_token.json'  # Store token in home directory

def authenticate_gmail():
    """Authenticate the user and return a Gmail API service object."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print(f"Error reading token file: {e}")
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing token: {e}")
                creds = None
        
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
                with open(TOKEN_FILE, 'w') as token:
                    token.write(creds.to_json())
                # Ensure proper permissions
                os.chmod(TOKEN_FILE, 0o600)
            except Exception as e:
                print(f"Error during OAuth flow: {e}")
                return None
    
    try:
        return build('gmail', 'v1', credentials=creds)
    except Exception as e:
        print(f"Error building Gmail service: {e}")
        return None

def send_email(service, recipient, subject, body):
    """Send an email using Gmail API."""
    if not service:
        print("No Gmail service available")
        return False

    message = MIMEText(body)
    message['to'] = recipient
    message['subject'] = subject
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    message = {'raw': raw_message}

    try:
        service.users().messages().send(userId="me", body=message).execute()
        print(f"Alert email sent successfully: {subject}")
        return True
    except Exception as e:
        print(f"Failed to send email: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Send Gmail alert')
    parser.add_argument('--subject', required=True, help='Email subject')
    parser.add_argument('--body', required=True, help='Email body')
    args = parser.parse_args()

    service = authenticate_gmail()
    return send_email(
        service,
        recipient='goodyear.judy@gmail.com',
        subject=args.subject,
        body=args.body
    )

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
