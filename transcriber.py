import os
import requests
import json
import logging
from io import BytesIO
from dotenv import load_dotenv
from supabase import create_client, Client
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
SUPABASE_URL: str = os.getenv("SUPABASE_URL")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY")
PROPEQUITY_API_BASE_URL: str = os.getenv("PROPEQUITY_API_BASE_URL")
SUPABASE_STORAGE_URL: str = os.getenv("SUPABASE_STORAGE_URL")

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Supabase and OpenAI Clients ---
# Ensure all required environment variables are set
if not all([SUPABASE_URL, SUPABASE_KEY, OPENAI_API_KEY, PROPEQUITY_API_BASE_URL, SUPABASE_STORAGE_URL]):
    logging.error("One or more required environment variables are not set. Please check your .env file.")
    exit(1) # Exit if essential variables are missing

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# --- Constants ---
MAX_FILE_COUNT = 53 # From the 'If' node condition
SUPABASE_TABLE_NAME = "propE_transcriber"
SUPABASE_STORAGE_BUCKET = "prope.transcriberaudio"

def run_transcriber_workflow():

    logging.info("Starting PropE_Transcriber workflow...")

    # 1. Get Count from Supabase (getcount & Summarize)
    try:
        response = supabase.table(SUPABASE_TABLE_NAME).select("file_id").neq("file_id", "null").execute()
        current_file_count = len(response.data)
        logging.info(f"Current file count in Supabase: {current_file_count}")

        # 2. Conditional Check (If node)
        if current_file_count >= MAX_FILE_COUNT:
            logging.warning(f"Count Complete! Current count ({current_file_count}) is >= {MAX_FILE_COUNT}. Stopping workflow.")
            return # Stop and Error
    except Exception as e:
        logging.error(f"Error checking Supabase count: {e}")
        return

    # 3. Get list of recordings from PropEquity API (Get-list)
    try:
        get_list_url = f"{PROPEQUITY_API_BASE_URL}/get-recordings"
        response = requests.get(get_list_url)
        response.raise_for_status() # Raise an exception for HTTP errors
        api_recordings = response.json()
        logging.info(f"Fetched {len(api_recordings)} recordings from PropEquity API.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching recordings from PropEquity API: {e}")
        return

    # 4. Get existing file_ids from Supabase (Supabase node)
    try:
        response = supabase.table(SUPABASE_TABLE_NAME).select("file_id").execute()
        existing_supabase_file_ids = {item['file_id'] for item in response.data}
        logging.info(f"Fetched {len(existing_supabase_file_ids)} existing file_ids from Supabase.")
    except Exception as e:
        logging.error(f"Error fetching existing file_ids from Supabase: {e}")
        return

    # 5. Compare Datasets to find new recordings
    new_recordings_to_process = []
    for record in api_recordings:
        # Assuming 'fileId' from API matches 'file_id' in Supabase
        if record.get('fileId') and record['fileId'] not in existing_supabase_file_ids:
            new_recordings_to_process.append(record)
    logging.info(f"Found {len(new_recordings_to_process)} new recordings to process.")

    if not new_recordings_to_process:
        logging.info("No new recordings to process. Workflow finished.")
        return

    # 6. Process each new recording
    for record_data in new_recordings_to_process:
        file_id = record_data.get('fileId')
        project_id = record_data.get('projectID')
        file_extension = record_data.get('fileExt')

        if not file_id:
            logging.warning(f"Skipping record due to missing fileId: {record_data}")
            continue

        logging.info(f"Processing file_id: {file_id}")

        try:
            # 6.1. Save metadata to Supabase (prope_transcriber_savefileid)
            supabase.table(SUPABASE_TABLE_NAME).insert({
                "file_id": file_id,
                "project_id": project_id,
                "file_extension": file_extension
            }).execute()
            logging.info(f"Metadata saved for {file_id} in Supabase.")

            # 6.2. Get Recording (Get-Recording)
            get_recording_url = f"{PROPEQUITY_API_BASE_URL}/{file_id}"
            audio_response = requests.get(get_recording_url, stream=True)
            audio_response.raise_for_status()
            audio_content = BytesIO(audio_response.content)
            logging.info(f"Downloaded audio for {file_id}.")

            # 6.3. Store in Supabase (store-in-supabase)
            # Determine content type (mimeType) from the downloaded file or assume based on extension
            # For simplicity, let's assume common audio types or try to infer
            mime_type = "audio/mpeg" # Default, adjust as needed based on file_extension
            if file_extension == "wav":
                mime_type = "audio/wav"
            elif file_extension == "mp4":
                mime_type = "audio/mp4" # Could be audio/mp4 or video/mp4

            storage_path = f"{file_id}" # Supabase storage path
            supabase.storage.from_(SUPABASE_STORAGE_BUCKET).upload(
                file=audio_content.getvalue(),
                path=storage_path,
                file_options={"content-type": mime_type, "x-upsert": "true"}
            )
            logging.info(f"Uploaded {file_id} to Supabase storage.")

            # 6.4. Make signed URL token (make-signedURL-token)
            # Supabase storage doesn't directly return a signed URL on upload.
            # We need to generate it separately.
            signed_url_response = supabase.storage.from_(SUPABASE_STORAGE_BUCKET).create_signed_url(
                path=storage_path,
                expires_in=3600 # 1 hour
            )
            signed_url = signed_url_response['signedURL']
            logging.info(f"Generated signed URL for {file_id}.")

            # 6.5. Save call recording URL (save-callrecording-URL)
            # Let's use the full signed_url directly.
            supabase.table(SUPABASE_TABLE_NAME ).update({
                "recording": signed_url
            }).eq("file_id", file_id).execute()
            logging.info(f"Updated recording URL for {file_id} in Supabase.")

            # 6.6. Convert file to binary (convertfiletobinary) - already have audio_content
            # This step is implicitly handled as we have the audio content in BytesIO.
            # For OpenAI, we need to pass the file-like object or bytes.

            # 6.7. Transcribe Call (transcribe-call)
            # Reset BytesIO stream to beginning for reading by OpenAI
            audio_content.seek(0)
            transcription_response = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=("audio.mp3", audio_content.getvalue(), mime_type), # filename, bytes, mimetype
                language="en",
                temperature=0.5
            )
            transcript_text = transcription_response.text
            logging.info(f"Transcribed audio for {file_id}.")

            # 6.8. Summarize Call (summarize-call)
            system_prompt = """You are an expert Indian real estate data analyst and you are well versed with the jargons and technicalities used in real estate transactions. You will be given call transcripts and you will have to provide a call summary along with following data in JSON format."""
            user_prompt = f"""Analyze this call transcript and extract the following information in JSON format:

Call Transcript: {transcript_text}

Required JSON Output:
{{
  "dto":
  {{
"Configuration": "",
"Size_Range": "",
"BSP": "",
"Total_Units": "",
"Units_available": "",
"Completion_Date": "",
"Additional_Notes": "",
"Notes": ""
}}
}}

Rules:
- If developer says "90% sold" and total is 100 units, then 10 units available
- Mark as "Successful" even if project is sold out but developer gave information
- Use "Successful (absorption)" if got price + availability info
- Calculate BSP by dividing price by carpet area
- All the responses should be string.

Instructions:
-"BSP": Calculate per square feet price by by dividing price of the individual configuration by carpet area of the same individual configuration. If there are two or three configuration available in the project then give the average value of the calculation

"Size_Range": The carpet of  the configuration as mentioned in the conversation. If there are two configuration 2 and 3 and they have carpet 750 and 1000 then record response as 750-1000.

"Units_available": Total units available for booking, as per the developer.

"Total_Units": Total units planned in the project.

"Configuration": List BHK types mentioned like "2,3".

"Completion_Date": Mention project completion time or say "Ready to Move" if completed.

"Additional_Notes": Expert summary of the whole transcript. This should include everything that was contained in the call.

"Notes": give response in one word, if the developer asked to call back or asks to connect with someone else by sharing a mobile number, mention as 'Call back', if wrong number was dialled then mention as 'Wrong number' or if the call went to voice mail or any automated answering machine, then mention as 'Voicemail'.if the information regarding the type of configuration, price and area of each configuration, total number of units the project will have and the units still available and/or total number of units sold so far has been provied, record response as 'Successful'.There is an exception for successful calls. If the developer does not give any information and tells the project is sold out or there is no units available for sale, such calls too should be labeled as 'Successful' If  the information of total number of units still available or total number of units sold so far is available  and price of units is available then label them as 'Successful (absorption)', if all the information is available except either the information of total number of units still available for sale or total number of units sold so far, then label them as 'Partial', please be aware that developer might either say 90% sold or 90% available, this means if there are 100 total units the 90% or 90 units are sold or 90 are still available out of 100 for sale.A call should be labelled as 'Unsuccessful' if there is no information available regarding total units that be there in the project, price of unit and  and how many units have been sold so far or are still available. ."""

            chat_completion = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"} # Request JSON output
            )
            # Let's get the content, then stringify it twice.
            summary_content_str = chat_completion.choices[0].message.content
            # Ensure it's valid JSON before double stringifying
            try:
                summary_json_obj = json.loads(summary_content_str)
                double_stringified_summary = json.dumps(json.dumps(summary_json_obj))
            except json.JSONDecodeError:
                logging.error(f"OpenAI did not return valid JSON for {file_id}. Raw content: {summary_content_str}")
                double_stringified_summary = json.dumps(summary_content_str) # Store as is if not valid JSON

            logging.info(f"Summarized call for {file_id}.")

            # 6.9. Update Supabase with transcriptData (updatewithcallresponse_direct2)
            supabase.table(SUPABASE_TABLE_NAME).update({
                "transcriptData": double_stringified_summary
            }).eq("file_id", file_id).execute()
            logging.info(f"Updated transcriptData for {file_id} in Supabase.")

            # 6.10. Send data back to PropEquity API (sendback-data-fromDB_direct)
            send_data_url = f"{PROPEQUITY_API_BASE_URL}/create-recording-transcript"
            payload_transcript_data = json.loads(double_stringified_summary) # This is the stringified JSON
            send_payload = {
                "fileId": file_id,
                "projectId": project_id,
                "transcriptData": payload_transcript_data, # This should be the stringified JSON
                "status": "1"
            }
            send_response = requests.post(send_data_url, json=send_payload)
            send_response.raise_for_status()
            logging.info(f"Sent data back to PropEquity API for {file_id}.")

            # 6.11. Update Supabase with callback response (updatewithcallresponse_direct)
            supabase.table(SUPABASE_TABLE_NAME).update({
                "callback_response": send_response.json() # Store the response from the API
            }).eq("file_id", file_id).execute()
            logging.info(f"Updated callback_response for {file_id} in Supabase.")

        except requests.exceptions.RequestException as e:
            error_message = f"HTTP Request Error for {file_id}: {e}"
            logging.error(error_message)
            # Error reporting path (error reporting & sendback-data-fromDB_direct2)
            handle_processing_error(file_id, project_id, error_message)
        except OpenAI.APIError as e:
            error_message = f"OpenAI API Error for {file_id}: {e}"
            logging.error(error_message)
            handle_processing_error(file_id, project_id, error_message)
        except Exception as e:
            error_message = f"General processing error for {file_id}: {e}"
            logging.error(error_message, exc_info=True) # Log traceback for general errors
            handle_processing_error(file_id, project_id, error_message)

    logging.info("PropE_Transcriber workflow completed.")

def handle_processing_error(file_id: str, project_id: str, error_details: str):
    """
    Handles errors during the processing of a single file, updating Supabase and
    sending error info back to the PropEquity API.
    """
    logging.info(f"Handling error for file_id: {file_id}")
    try:
        # Update Supabase with error (error reporting)
        supabase.table(SUPABASE_TABLE_NAME).update({
            "transcriptData": json.dumps({"error": error_details}) # Store error as JSON string
        }).eq("file_id", file_id).execute()
        logging.info(f"Updated Supabase with error for {file_id}.")

        # Send error back to PropEquity API (sendback-data-fromDB_direct2)
        send_data_url = f"{PROPEQUITY_API_BASE_URL}/create-recording-transcript"
        send_payload = {
            "fileId": file_id,
            "projectId": project_id,
            "transcriptData": json.dumps({"error": error_details}), # Send error as stringified JSON
            "status": "0" # Assuming '0' indicates an error status
        }
        send_response = requests.post(send_data_url, json=send_payload)
        send_response.raise_for_status()
        logging.info(f"Sent error data back to PropEquity API for {file_id}.")

        # Update Supabase with callback response (updatewithcallresponse_direct)
        supabase.table(SUPABASE_TABLE_NAME).update({
            "callback_response": send_response.json()
        }).eq("file_id", file_id).execute()
        logging.info(f"Updated callback_response with error details for {file_id} in Supabase.")

    except Exception as e:
        logging.error(f"Critical error during error handling for {file_id}: {e}", exc_info=True)

if __name__ == "__main__":
    run_transcriber_workflow()
