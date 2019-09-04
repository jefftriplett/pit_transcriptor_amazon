import boto3
import click
import json
import logging
import os
import requests
import time

from pathlib import Path
from slugify import slugify


bucket = os.environ.get("BUCKET_NAME")
storage = boto3.client("s3")
transcribe = boto3.client("transcribe")


@click.group()
@click.version_option()
def cli():
    "Transcribe an audio file"


@cli.command()
@click.argument(
    "filename",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
def json_builder(filename):

    with open(filename) as json_file:
        transcript_json = json.load(json_file)
        logging.debug(transcript_json["results"]["transcripts"][0]["transcript"])

    json_results = transcript_json["results"]
    channels = json_results["channel_labels"]["channels"]

    voices = {"ch_0": "speaker 1", "ch_1": "speaker 2"}
    speaker = voices["ch_0"]
    text_lines = [f"{speaker}\n"]

    for item in json_results["items"]:

        for channel in channels:
            if item in channel["items"]:
                ch = channel["channel_label"]
                content = item["alternatives"][0]["content"]

                if item["type"] != "punctuation":
                    if speaker != voices[ch]:
                        speaker = voices[ch]
                        start_time = round(float(item["start_time"]))
                        text_lines.append(f"\n\n{speaker}: {start_time}\n")

                    if float(item["alternatives"][0]["confidence"]) < 0.85:
                        content = f"%%{content}"

                elif text_lines[-1] == content:
                    continue

                text_lines.append(content)

    output_filename = filename.replace(".json", ".txt")
    with open(output_filename, "w") as transcript:
        content = " ".join(text_lines)
        content = content.replace(".", ".\n\n")
        # content, count = re.subn(r" (?=[\.\,\?\!])", "\n", content)
        # content, count = re.subn(r" (?=[\.\?\!])", "\n", content)
        # print(count)
        transcript.write(content)


@cli.command()
@click.argument(
    "filename",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option("--delay", default=30)
def start_transcription(delay, filename):
    stem = slugify(Path(filename).stem)
    suffix = Path(filename).suffix
    key = f"{stem}{suffix}"
    transcribe_job_uri = f"{storage.meta.endpoint_url}/{bucket}/{key}"

    click.echo(transcribe_job_uri)

    transcribe.start_transcription_job(
        TranscriptionJobName=key,
        Media={"MediaFileUri": transcribe_job_uri},
        MediaFormat=suffix[1:],
        LanguageCode="en-US",
        Settings={"ChannelIdentification": True},
    )


@cli.command()
@click.argument(
    "filename",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
@click.option("--delay", default=30)
def transcription(delay, filename):
    stem = slugify(Path(filename).stem)
    suffix = Path(filename).suffix
    key = f"{stem}{suffix}"
    transcribe_job_uri = f"{storage.meta.endpoint_url}/{bucket}/{key}"

    click.echo(transcribe_job_uri)

    # transcribe.start_transcription_job(
    #     TranscriptionJobName=key,
    #     Media={"MediaFileUri": transcribe_job_uri},
    #     MediaFormat=suffix[1:],
    #     LanguageCode="en-US",
    #     Settings={"ChannelIdentification": True},
    # )

    click.echo("transcription started")

    job = transcribe.get_transcription_job(TranscriptionJobName=key)

    while job["TranscriptionJob"]["TranscriptionJobStatus"] == "IN_PROGRESS":
        time.sleep(delay)
        job = transcribe.get_transcription_job(TranscriptionJobName=key)

    r = requests.get(job["TranscriptionJob"]["Transcript"]["TranscriptFileUri"])
    r.raise_for_status()

    with open(f"{stem}.json", "w") as json_file:
        json_file.write(json.dumps(r.json(), indent=2))


@cli.command()
@click.argument(
    "filename",
    type=click.Path(file_okay=True, dir_okay=False, allow_dash=False),
    required=True,
)
def upload(filename):
    stem = slugify(Path(filename).stem)
    suffix = Path(filename).suffix
    key = f"{stem}{suffix}"
    upload = storage.upload_file(Filename=filename, Bucket=bucket, Key=key)
    click.echo("Audio Uploaded, Beginning Transcription")
    print(upload)


if __name__ == "__main__":
    cli()
