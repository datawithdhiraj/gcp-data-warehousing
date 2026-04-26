import requests, json


def push_email_notification(flag, response_status_code=0, error=""):

    external_data = {
        "from": {
            "email": "transactions.abcd@abcd.org",
            "name": "Notification"
        },
        "subject": "[PROD ALERT] : Sampark Failure Notification",
        "content": [
            {
                "type": "html",
                "value": f"""Hi All, <br><br>
                This is to notify that the cloud function responsible for transitioning data from bucket to raw layer for the sampark has failed with below error : <br><br> <b>{error}</b>
                <br><br> Thanks, <br>GCP Tech Team"""
            }
        ],
        "personalizations": [
            {
                "to": [
                    {
                        "email": "xyz@abcd.com",
                        "name": "xyz"
                    },
                    {
                        "email": "xyz1@abcd.com",
                        "name": "xyz1"
                    }
                ]
            }
        ]
    }

    internal_data = {
        "from": {
            "email": "transactions.abcd@digitalabcd.org",
            "name": "Notification"
        },
        "subject": "[PROD ALERT] : Sampark Failure Notification",
        "content": [
            {
                "type": "html",
                "value": f"""Hi All, <br><br>
                This is to notify that the cloud function responsible for transitioning data from bucket to raw layer for the sampark has failed with below error : <br><br> <b>{error}</b>
                <br><br> Thanks, <br>GCP Tech Team"""
            }
        ],
        "personalizations": [
            {
                "to": [
                    {
                        "email": "xyz@abcd.com",
                        "name": "xyz"
                    },
                    {
                        "email": "xyz1@abcd.com",
                        "name": "xyz1"
                    }
                ]
            }
        ]
    }

    try:
        push_email_access_token = email_access_token()

        email_url = 'https://api.abcd.com/netcore/email'

        headers = {
            'Content-Type': 'application/json',
            'Content-Length': '0',
            'Accept-Encoding': 'gzip,deflate,br',
            'channel': '1',
            'auth-token': push_email_access_token,
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'User-Agent': 'request',
        }

        if flag == 'external':
            data = external_data
        else:
            data = internal_data

        response = requests.post(email_url, headers=headers, json=data)

        print(response.text)

    except Exception as e:
        raise Exception(f"Error in push_email_notification function: {str(e)}")


def email_access_token():
    try:
        access_token_url = 'https://abfss-prod-api.auth.ap-south-1.amazoncognito.com/oauth2/token'

        data = {
            'grant_type': 'client_credentials',
            'client_id': '5td5tck1fjderq8ga7dig1h345',
            'scope': 'netcore/auth'
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': '0',
            'Accept-Encoding': 'gzip,deflate,br',
            'Connection': 'keep-alive',
            'Authorization': 'Basic NXRkNXRjazFmamRlcnE4Z2E3ZGlnMWgzNDU6MXI4Ymc5NTFpZXU2YnV0bGhjdGNmYWxsMXJjZjBiZXEzM3NvMHZvcTh2MjFkZzdkZHVqOQ=='
        }

        response = requests.post(access_token_url, headers=headers, data=data)
        response_json = json.loads(response.text)

        access_token = response_json.get('access_token')

        return access_token

    except Exception as e:
        raise Exception(f"Error in email_access_token function: {str(e)}")