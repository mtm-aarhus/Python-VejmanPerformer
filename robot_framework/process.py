from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
import os
import json
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import requests
import time
import re
import pyodbc

def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    orchestrator_connection = OrchestratorConnection("VejManPerformer", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)
    orchestrator_connection.log_info("Started process")

    RobotCredentials = orchestrator_connection.get_credential('Robot365User')
    username = RobotCredentials.username
    password = RobotCredentials.password

    queue_item = orchestrator_connection.get_next_queue_element('VejmanPerformer')
    if not queue_item:
        orchestrator_connection.log_info("No new queue items to process.")
        exit()
    json_obj = json.loads(queue_item.data)

    orchestrator_connection.log_info("Assigning variables")

    Tilladelse = json_obj.get('case_number')
    Adresse = json_obj.get('vejnavn')
    CaseID = json_obj.get('case_id')
    Folder = json_obj.get('sharepoint_folder')
    DownloadFolder = os.path.join(os.path.expanduser("~"), "Downloads")
    VejmanToken = orchestrator_connection.get_credential("VejmanToken").password

    orchestrator_connection.log_info(f'Starter Vejmanproces for tilladelse nr. {Tilladelse}')

    SharePointUrl = orchestrator_connection.get_constant('AarhusKommuneSharePoint').value + '/Teams/tea-teamsite10014'
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads", "VejmanPerformer")  ###No need for folder

    #Create sharepoint connection to folder
    credentials = UserCredential(username, password)
    ctx = ClientContext(SharePointUrl).with_credentials(credentials)

    #Create path to folder
    folder = ctx.web.get_folder_by_server_relative_url(Folder)  ##Man kan execute i slutningen af linjen

    #Randomnumber making
    RandomNum = str(int(time.time()*1000))
    vejman_url = "https://vejman.vd.dk/permissions/getcase?caseid="+CaseID+"&cachebuster="+RandomNum+"&token="+VejmanToken

    # Make the HTTP GET request
    response = requests.get(vejman_url, timeout=60)
    response.raise_for_status()

    data = json.loads(response.text).get('data')
    approved_files = []
    approved_nonces = []

    #Connecting to the SQL-server:
    sql_server = orchestrator_connection.get_constant("SqlServer")
    conn_string = "DRIVER={SQL Server};"+f"SERVER={sql_server.value};DATABASE=PYORCHESTRATOR;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""
                SELECT NONCE, [FILE] FROM [dbo].[VejmanVedlaeg]
                WHERE ID = ? 
            """, (CaseID,))
    rows = cursor.fetchall()

    # Convert database rows to a dictionary {nonce: filename}
    existing_files = {str(row[0]): row[1] for row in rows}

    # Get nonces from new attachments
    att = data['attachments']
    new_nonces = {str(item['nonce']) for item in att}

    # Process new attachments
    for item in att:
        if 'approved' not in item:
            approved = False
        else:
            approved = item['approved']
        Nonce = str(item['nonce'])  # Ensure it's a string for comparison
        Filename = item['file_name']
        Filename_start, Filename_end = Filename.rsplit('.', 1)
        Filename_start = sanitize_file_name(Filename_start.replace('.', ""))
        Filename = f'{Filename_start}.{Filename_end}' 
        FileID = item['id']
        FileURL = f"https://vejman.vd.dk/permissions/getfile?fileid={FileID}&nonce={Nonce}&token={VejmanToken}"

        if ".msg" in Filename:
            continue

        if Nonce not in existing_files:
            if approved:
                conn.commit()  # Save changes to the database
                orchestrator_connection.log_info(f"Added new row with ID '{CaseID}', nonce '{Nonce}' and Filename '{Filename}'")

                download_and_upload_file_to_sharepoint(orchestrator_connection, FileURL, ctx, Filename)
                cursor.execute("""
                    INSERT INTO [dbo].[VejmanVedlaeg] (ID, NONCE, [FILE])
                    VALUES (?, ?, ?)
                """, (CaseID, Nonce, Filename))
                conn.commit()
        else:
            # If already in existing nonces:
            if not approved:
                # Deleting from SharePoint
                delete_file_if_exists(f'{folder}/{Filename}', ctx)
                
                # Deleting from the database
                cursor.execute("""
                    DELETE FROM [dbo].[VejmanVedlaeg] 
                    WHERE NONCE = ? AND ID = ?
                """, (Nonce, CaseID))
                conn.commit()

    missing_nonces = set(existing_files.keys()) - new_nonces  # Find nonces that exist in DB but not in `att`

    for missing_nonce in missing_nonces:
        missing_filename = existing_files[missing_nonce]  # Get the filename associated with the nonce

        # Delete the file from SharePoint
        delete_file_if_exists(f'{folder}/{missing_filename}', ctx)

        # Remove entry from the database
        cursor.execute("""
            DELETE FROM [dbo].[VejmanVedlaeg] 
            WHERE NONCE = ? AND ID = ?
        """, (missing_nonce, CaseID))
        conn.commit()

        orchestrator_connection.log_info(f"Deleted missing file '{missing_filename}' with nonce '{missing_nonce}' from SharePoint and database.")

    # Close the connection
    cursor.close()
    conn.close()       

def delete_file_if_exists(ctx, file_relative_url):
    try:
        file = File.from_url(file_relative_url)
        file.delete_object()
        ctx.execute_query()
        orchestrator_connection.log_info(f"File '{file_relative_url}' successfully deleted!")
    except Exception as e:
        orchestrator_connection.log_info(f"File not found or cannot be deleted: {str(e)}")
def sanitize_file_name(file_name):
    pattern = r'[~#%&*{}\[\]\\:<>?/+|$¤£€\"\t]'
    file_name = re.sub(pattern, "", file_name)
    file_name = re.sub(r"\s+", " ", file_name).strip()
    return file_name
def download_and_upload_file_to_sharepoint(orchestrator_connection: OrchestratorConnection, FileURL, ctx: ClientContext, Filename ):
    # Start downloading with a progress bar
    response = requests.get(FileURL, stream=True, timeout = 60)
    response.raise_for_status()  

    total_size = int(response.headers.get("content-length", 0))  
    block_size = 8192  # Download in chunks of 8KB

    with open(Filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=block_size):
            file.write(chunk)

    orchestrator_connection.log_info(f"{Filename} downloaded successfully to: {Filename}")

    with open(Filename, "rb") as file_content:
        folder.files.add(Filename, file_content, True)  
        ctx.execute_query()

        orchestrator_connection.log_info(f"Uploaded: {Filename} to {Folder}")