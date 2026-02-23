from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import json
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
import requests
import time
import re
import pyodbc


def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    info_logs = []

    def log_info(msg: str):
        info_logs.append(msg)

    try:
        RobotCredentials = orchestrator_connection.get_credential('Robot365User')
        username = RobotCredentials.username
        password = RobotCredentials.password

        json_obj = json.loads(queue_element.data)

        log_info("Assigning variables")

        Tilladelse = json_obj.get('case_number')
        CaseID = json_obj.get('case_id')
        Folder = json_obj.get('sharepoint_folder')
        VejmanToken = orchestrator_connection.get_credential("VejmanToken").password

        log_info(f'Starter Vejmanproces for tilladelse nr. {Tilladelse}')

        SharePointUrl = orchestrator_connection.get_constant('AarhusKommuneSharePoint').value + '/Teams/tea-teamsite10014'

        credentials = UserCredential(username, password)
        ctx = ClientContext(SharePointUrl).with_credentials(credentials)

        certification = orchestrator_connection.get_credential("SharePointCert")
        api = orchestrator_connection.get_credential("SharePointAPI")

        cert_credentials = {
            "tenant": api.username,
            "client_id": api.password,
            "thumbprint": certification.username,
            "cert_path": certification.password
        }
        ctx = ClientContext(SharePointUrl).with_client_certificate(**cert_credentials)

        ctx.web.get_folder_by_server_relative_url(Folder)

        RandomNum = str(int(time.time() * 1000))
        vejman_url = (
            "https://vejman.vd.dk/permissions/getcase?"
            f"caseid={CaseID}&cachebuster={RandomNum}&token={VejmanToken}"
        )

        response = requests.get(vejman_url, timeout=60)
        response.raise_for_status()

        data = json.loads(response.text).get('data')

        sql_server = orchestrator_connection.get_constant("SqlServer")
        conn_string = (
            "DRIVER={SQL Server};"
            f"SERVER={sql_server.value};DATABASE=PYORCHESTRATOR;Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT NONCE, [FILE] FROM [dbo].[VejmanVedlaeg]
            WHERE ID = ?
        """, (CaseID,))
        rows = cursor.fetchall()

        existing_files = {str(row[0]): row[1] for row in rows}

        att = data['attachments']
        new_nonces = {str(item['id']) + '_' + str(item['nonce']) for item in att}

        for item in att:
            approved = item.get('approved', False)
            Nonce = str(item['nonce'])
            Filename = item['file_name']

            log_info(Filename)

            try:
                Filename_start, Filename_end = Filename.rsplit('.', 1)
            except Exception:
                Filename_start = Filename
                Filename_end = "pdf"

            Filename_start = sanitize_file_name(Filename_start.replace('.', ""))
            Filename = f'{Filename_start}.{Filename_end}'

            FileID = item['id']
            FileURL = (
                "https://vejman.vd.dk/permissions/getfile?"
                f"fileid={FileID}&nonce={Nonce}&token={VejmanToken}"
            )
            ID_Nonce = str(FileID) + '_' + Nonce

            if ".msg" in Filename:
                continue

            if ID_Nonce not in existing_files:
                if approved:
                    conn.commit()

                    log_info(
                        f"Added new row with ID '{CaseID}', nonce '{ID_Nonce}' and Filename '{Filename}'"
                    )

                    download_and_upload_file_to_sharepoint(
                        FileURL, ctx, Filename, Folder, log_info
                    )

                    cursor.execute("""
                        INSERT INTO [dbo].[VejmanVedlaeg] (ID, NONCE, [FILE])
                        VALUES (?, ?, ?)
                    """, (CaseID, ID_Nonce, Filename))
                    conn.commit()
            else:
                if not approved:
                    delete_file_if_exists(
                        f'{Folder}/{Filename}', ctx, orchestrator_connection
                    )

                    cursor.execute("""
                        DELETE FROM [dbo].[VejmanVedlaeg]
                        WHERE NONCE = ? AND ID = ?
                    """, (ID_Nonce, CaseID))
                    conn.commit()

        missing_nonces = set(existing_files.keys()) - new_nonces

        for missing_nonce in missing_nonces:
            missing_filename = existing_files[missing_nonce]

            delete_file_if_exists(
                f'{Folder}/{missing_filename}', ctx, orchestrator_connection
            )

            cursor.execute("""
                DELETE FROM [dbo].[VejmanVedlaeg]
                WHERE NONCE = ? AND ID = ?
            """, (missing_nonce, CaseID))
            conn.commit()

            log_info(
                f"Deleted missing file '{missing_filename}' with nonce '{missing_nonce}' from SharePoint and database."
            )

        cursor.close()
        conn.close()

    except Exception as e:
        for msg in info_logs:
            orchestrator_connection.log_info(msg)

        orchestrator_connection.log_error(str(e))
        raise


def delete_file_if_exists(file_relative_url, ctx, orchestrator_connection):
    try:
        file = ctx.web.get_file_by_server_relative_url(file_relative_url)
        file.delete_object()
        ctx.execute_query()
    except Exception as e:
        orchestrator_connection.log_error(
            f"Failed to delete file '{file_relative_url}': {str(e)}"
        )
        raise


def sanitize_file_name(file_name):
    pattern = r'[~#%&*{}\[\]\\:<>?/+|$¤£€\"\t]'
    file_name = re.sub(pattern, "", file_name)
    file_name = re.sub(r"\s+", " ", file_name).strip()
    return file_name


def download_and_upload_file_to_sharepoint(FileURL, ctx, Filename, folder, log_info):
    response = requests.get(FileURL, stream=True, timeout=60)
    response.raise_for_status()

    block_size = 8192

    with open(Filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=block_size):
            file.write(chunk)

    log_info(f"{Filename} downloaded successfully to: {Filename}")

    sharepoint_folder = ctx.web.get_folder_by_server_relative_url(folder)
    with open(Filename, "rb") as file_content:
        sharepoint_folder.files.add(Filename, file_content, True)
        ctx.execute_query()

    log_info(f"Uploaded: {Filename} to {folder}")
