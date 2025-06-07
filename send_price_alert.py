from google.cloud import secretmanager,bigquery
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
import logging

# set the logging config
logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

project_id = 'e-object-459802-s8'
secret_id = 'sendgrid-api-key'

bq = bigquery.Client(project=project_id)

# get secret-api-key
def get_secret(secret, secret_version='latest'):
    client = secretmanager.SecretManagerServiceClient()
    name = client.secret_version_path(
        project=project_id,
        secret=secret,
        secret_version=secret_version
    )
    response = client.access_secret_version(name=name)
    return response.payload.data.decode('UTF-8')


def log_alert(row):
    query = f"""
    INSERT INTO `e-object-459802-s8.my_dataset.alert_history` (user_id, coin, price, direction, threshold, alert_time)
    VALUES ('{row.user_id}', '{row.coin}', {row.alerted_price}, '{row.direction}', {row.threshold}, TIMESTAMP('{row.alert_time}'))
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING", row.user_id),
            bigquery.ScalarQueryParameter(None, "STRING", row.coin),
            bigquery.ScalarQueryParameter(None, "FLOAT", row.alerted_price),
            bigquery.ScalarQueryParameter(None, "STRING", row.direction),
            bigquery.ScalarQueryParameter(None, "FLOAT", row.threshold),
            bigquery.ScalarQueryParameter(None, "TIMESTAMP", row.alert_time),
        ]
    )
    bq.query(query, job_config=job_config).result()
    logging.info(f"Inserted alert history for user {row.user_id}, coin {row.coin}")
    return "Data inserted in alert history"


def already_alerted(row):
    # check for a recent alert sent to this user for the same coin and direction
    query = f"""
    SELECT 1 FROM `e-object-459802-s8.my_dataset.alert_history`
    WHERE user_id = '{row.user_id}'
      AND coin = '{row.coin}'
      AND direction = '{row.direction}'
      AND alert_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    LIMIT 1
    """
    results = bq.query(query).result()
    return any(results)  # True if record exists


def send_email(email,coin,alerted_price,threshold,direction):
  try:
    sg = SendGridAPIClient(api_key=get_secret(secret=secret_id))
    from_email = Email("email registered in sendgrid>")  # Change to your verified sender
    to_email = To(email)  # Change to your recipient
    subject = f"Price alert for coin - {coin}"
    content = Content("text/plain", f"The price of {coin} is {direction} of the threshold-{threshold} set. New price is {alerted_price}")
    mail = Mail(from_email, to_email, subject, content)

    # Get a JSON-ready representation of the Mail object
    mail_json = mail.get()

    # Send an HTTP POST request to /mail/send
    response = sg.client.mail.send.post(request_body=mail_json)
    logging.info(f"Alert sent to {email} for coin {coin}")
    return "Mail sent"
  except Exception as e:
    # print(f"Exception: {e}")
    logging.error(f"Failed to send email to {to_email}: {e}", exc_info=True)
    return f"Error with sending mail: {e}"

def send_alerts(request):
    try:
        query = """
        SELECT * FROM `e-object-459802-s8.my_dataset.crypto_threshold_violations`
        """
        results = bq.query(query).result()
        logging.info("Fetched rows from crypto_threshold_violations table.")

        for row in results:
            try:
                if not already_alerted(row):
                    send_email(email=row.email, coin=row.coin, alerted_price=row.alerted_price, threshold=row.threshold, direction=row.direction)
                    log_alert(row)
                    logging.info(f"Processed alert for user {row.user_id}, coin {row.coin}")
                else:
                    logging.warning(f"Skipped alert for user {row.user_id}, coin {row.coin}: Already alerted")
            except Exception as e:
                logging.error(f"Failed to process row for user {row.user_id}, coin {row.coin}: {e}", exc_info=True)
        return "All Rows processed"
    except Exception as e:
        logging.error(f"Error occurred - {e}", exc_info=True)
        return f"Error: {e}"



