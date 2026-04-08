"""Pipeline: messenger_message – actions to run when a Messenger message arrives."""

from pipeline.actions.smartmoving_note import send_messenger_note

ACTIONS = [
    send_messenger_note,
]
