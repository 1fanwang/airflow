> Part of the workspace knowledge base. See [CLAUDE.md](../../CLAUDE.md) for the navigation map.

# Email Integration

## Sending Email from Airflow DAGs

### SMTP Gateway

LinkedIn provides an internal SMTP gateway for sending email from DAGs and other services:

**Gateway Details:**
- **Host**: `mail-gw.corp.linkedin.com`
- **Port**: 25
- **Protocol**: SMTP with STARTTLS
- **Authentication**: Not required for internal @linkedin.com email addresses
- **Reference**: [How To Send Email Messages On Our Network](https://linkedin.atlassian.net/wiki/spaces/IST/pages/607259230)

### Example Usage with PythonOperator

```python
import smtplib
from email.mime.text import MIMEText

def send_email_task():
    with smtplib.SMTP('mail-gw.corp.linkedin.com', 25) as server:
        server.starttls()
        msg = MIMEText('Email body')
        msg['Subject'] = 'Subject Line'
        msg['From'] = 'sender@linkedin.com'
        msg['To'] = 'recipient@linkedin.com'
        server.send_message(msg)
```

### Testing

Test email functionality on **airflow-load-test** cluster / **airflow-test** K8s namespace. Use `airflow_load_test` database for simulation to avoid production impact.
