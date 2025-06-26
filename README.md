# PropE Transcriber Workflow

This Python script automates the process of transcribing and summarizing real estate call recordings. It fetches new recordings, uploads them to Supabase storage, transcribes them using OpenAI's Whisper model, summarizes the content using OpenAI's GPT model, and updates a Supabase database and an external API with the results.

## Features

*   **Automated Recording Ingestion:** Fetches new call recordings from a specified API endpoint.
*   **Secure Storage:** Uploads recordings to Supabase Storage.
*   **Audio Transcription:** Transcribes audio files into text using OpenAI's Whisper.
*   **Intelligent Summarization:** Summarizes call transcripts and extracts key real estate data (e.g., configuration, pricing, availability) using OpenAI's GPT-4o-mini, formatted as JSON.
*   **Data Management:** Updates a Supabase database with recording metadata, transcription, and summary data.
*   **External API Integration:** Sends processed data back to a PropEquity API endpoint.
*   **Error Handling:** Includes mechanisms to report and log errors during processing.

## Prerequisites

Before you begin, ensure you have the following:

*   **Python 3.8+** installed on your system.
*   **Git** installed.
*   An **OpenAI API Key**.
*   A **Supabase Project** with:
    *   A table named propE_transcriber
    *   A storage bucket named `prope.transcriberaudio` (you'll need to create this).
    *   Your Supabase Project URL and Anon Key.

## Setup Instructions

Follow these steps to get the project up and running on your local machine.

### 1. Clone the Repository

First, clone this GitHub repository to your local machine:

```bash
git clone https://github.com/invortoai/prope_transcriber.git
cd prope_transcriber
