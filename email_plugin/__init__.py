import os
import os.path
import smtplib
import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

import pytest
import py.xml

from tools.keystore import KeyStore
from tools.marks import get_version

LOGGER = logging.getLogger(__name__)

DEFAULT_EMAIL_BODY_TPL = '''<!DOCTYPE html>
    <html lang="en">
    <head>
        <title>{subject}</title>
        <style>
            .blue   {{ color: blue; }}
            .fbold  {{ font-weight: bold; }}
            .red    {{ color: red; }}
            .green  {{ color: green; }}
            .tan    {{ color: tan; }}
            .lightgreen {{ color: #90EE90; }}
            .orange {{ color: orange; }}
            .black  {{ color: black; }}
            .fnormal {{ font-weight: normal; }}
            .notice {{ font-size:120%; }}
            .small {{ font-size:80%; }}
            #results_table {{
                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                width: 50%;
            }}
            #results_table td, #results_table th {{
                border: 1px solid #ddd;
                padding: 8px;

            }}
            #results_table tr:nth-child(even) {{ background-color: #f2f2f2;}}
            #results_table tr:hover {{ background-color: #ddd; }}
            #results_table th {{
                padding-top: 12px;
                padding-bottom: 12px;
                text-align: left;
                background-color: #85C1E9;
                color: white;
            }}

            .result_table {{
                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                vertical-align: top;
                width: 50%;
            }}

            .result_table tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}

            .result_table td, .result_table th {{
                border: 1px solid #ddd;
                text-align: center;
            }}

            .result_table th {{
                padding: 8px;
                text-align: center;
                background-color: #85C1E9;
                color: white;
            }}

            .result_table_error {{
                padding: 8px;
                text-align: center;
                background-color: red;
                color: white;
            }}

        </style>
    </head>
    <body>
        <h3>Test details</h3>
        <div>
            <ul>
                <li><span class="fbold">Scylla Version:</span> {scylla_version}</li>
                <li><span class="fbold">Manager Version:</span> {manager_version}</li>
                <li><span class="fbold">Build Id:</span> {build_id}</li>
            </ul>
        </div>

        <h3>
            <span>Test result</span>
        </h3>
            <table class='result_table'>
                <tr>
                    <th>Total</th>
                    <th>Pass</th>
                    <th>Fail</th>
                    <th>Skip</th>
                    <th>Error</th>
                    <th>xPassed</th>
                    <th>xFailed</th>
                </tr>
                <tr>
                    <td>{total}</td>
                    <td style="color:green">{passed}</td>
                    <td style="color:red">{failed}</td>
                    <td>{skipped}</td>
                    <td style="color:red">{error}</td>
                    <td>{xpassed}</td>
                    <td>{xfailed}</td>
                </tr>
            </table>

    <h3>Links:</h3>
    <ul>
            <li><a href={build_url}>Build URL</a></li>
            <li><a href="https://70f106c98484448dbc4705050eb3f7e9.us-east-1.aws.found.io:9243/app/dashboards#/view/e6a92910-3a3b-11ec-82e2-5720a4e10573?_g=(filters:!((query:(match_phrase:(build_tag.keyword:{build_tag})))),refreshInterval:(pause:!t,value:0),time:(from:now%2Fd,to:now%2Fd))">Kibana Dashboard</a></li>
    </ul>

    </body>
</html>'''


def pytest_addoption(parser):
    parser.addoption("--email", action='store', help="Send email when --email")


def pytest_html_results_table_header(cells):
    cells.pop()


def pytest_html_results_table_row(report, cells):
    cells.pop()


def pytest_html_results_table_html(report, data):
    if report.passed:
        del data[:]
        data.append(py.xml.html.div("No log output captured.", class_="empty log"))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    report.description = item.function.__doc__ if item.function.__doc__ else ''


def pytest_terminal_summary(terminalreporter, config):
    email_recipients = config.getoption("--email")
    email_recipients = email_recipients.split(',') if email_recipients else None
    if email_recipients:
        stats = {}
        stats['passed'] = len(terminalreporter.stats.get('passed', ''))
        stats['failed'] = len(terminalreporter.stats.get('failed', ''))
        stats['skipped'] = len(terminalreporter.stats.get('skipped', ''))
        stats['error'] = len(terminalreporter.stats.get('error', ''))
        stats['xpassed'] = len(terminalreporter.stats.get('xpassed', ''))
        stats['xfailed'] = len(terminalreporter.stats.get('xfailed', ''))
        stats['total'] = sum(stats.values())

        # remove duplication, so the report would be clearer to read
        for test_report in terminalreporter.stats.get('error', []):
            if test_report.nodeid in [i.nodeid for i in terminalreporter.stats.get('passed', [])] or \
                    test_report.nodeid in [i.nodeid for i in terminalreporter.stats.get('failed', [])]:
                stats['total'] -= 1

        html_attachment = config.getoption("--html")
        stats.update(get_ci_info())
        stats['scylla_version'] = str(get_version(config.getvalue(
            '--cassandra-dir'), config.getvalue('--scylla-version')))
        stats['manager_version'] = config.getoption("--scylla-manager-package") or "N/A"

        stats['status'] = "FAILED" if stats['failed'] or stats['error'] else "SUCCESS"
        subject = stats['subject'] = f"{stats['status']}: {stats['job_name']} {stats['build_id']} - {datetime.now().isoformat(' ', 'seconds')}"
        html = DEFAULT_EMAIL_BODY_TPL.format(**stats)

        email_client = Email()
        LOGGER.info("Sending email to '%s'", email_recipients)

        email_client.send(subject=subject,
                          content=html,
                          recipients=email_recipients,
                          files=(html_attachment, ) if html_attachment else ())


class AttachementSizeExceeded(Exception):
    def __init__(self, current_size, limit):
        self.current_size = current_size
        self.limit = limit
        super().__init__()


class BodySizeExceeded(Exception):
    def __init__(self, current_size, limit):
        self.current_size = current_size
        self.limit = limit
        super().__init__()


class Email:
    #  pylint: disable=too-many-instance-attributes
    """
    Responsible for sending emails
    """
    _attachments_size_limit = 10485760  # 10Mb = 20 * 1024 * 1024
    _body_size_limit = 26214400  # 25Mb = 20 * 1024 * 1024

    def __init__(self):
        self.sender = "qa@scylladb.com"
        self._password = ""
        self._user = ""
        self._server_host = "smtp.gmail.com"
        self._server_port = "587"
        self._conn = None
        self._retrieve_credentials()
        self._connect()

    def _retrieve_credentials(self):
        keystore = KeyStore()
        creds = keystore.get_email_credentials()
        self._user = creds["user"]
        self._password = creds["password"]

    def _connect(self):
        self.conn = smtplib.SMTP(host=self._server_host, port=self._server_port)
        self.conn.ehlo()
        self.conn.starttls()
        self.conn.login(user=self._user, password=self._password)

    def prepare_email(self, subject, content, recipients, html=True, files=()):  # pylint: disable=too-many-arguments
        msg = MIMEMultipart()
        msg['subject'] = subject
        msg['from'] = self.sender
        assert recipients, "No recipients provided"
        msg['to'] = ','.join(recipients)
        if html:
            text_part = MIMEText(content, "html")
        else:
            text_part = MIMEText(content, "plain")
        msg.attach(text_part)
        attachment_size = 0
        for path in files:
            attachment_size += os.path.getsize(path)
            with open(path, "rb") as fil:
                part = MIMEApplication(
                    fil.read(),
                    Name=os.path.basename(path)
                )
            part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(path)
            msg.attach(part)
        if attachment_size >= self._attachments_size_limit:
            raise AttachementSizeExceeded(current_size=attachment_size, limit=self._attachments_size_limit)
        email = msg.as_string()
        if len(email) >= self._body_size_limit:
            raise BodySizeExceeded(current_size=len(email), limit=self._body_size_limit)
        return email

    def send(self, subject, content, recipients, html=True, files=()):  # pylint: disable=too-many-arguments
        """
        :param subject: text
        :param content: text/html
        :param recipients: iterable, list of recipients
        :param html: True/False
        :param files: paths of the files that will be attached to the email
        :return:
        """
        email = self.prepare_email(subject, content, recipients, html, files)
        self.send_email(recipients, email)

    def send_email(self, recipients, email):
        self.conn.sendmail(self.sender, recipients, email)

    def __del__(self):
        self.conn.quit()


def get_ci_info():
    return dict(
        build_url=os.getenv("BUILD_URL", "N/A"),
        build_id=os.getenv("BUILD_DISPLAY_NAME", "N/A"),
        build_tag=os.getenv("BUILD_TAG", "N/A"),
        job_name=os.getenv("JOB_NAME", "N/A"),
    )
