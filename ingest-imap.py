# Databricks notebook source
import imaplib
import email
from email.header import decode_header
import webbrowser
import os

# account credentials
username = "alex@exalake.com"
password = "WPWW@ss1029231"

# create an IMAP4 class with SSL 
imap = imaplib.IMAP4_SSL("imap.gmail.com")
# authenticate
imap.login(username, password)

def clean(text):
    # clean text for creating a folder
    return "".join(c if c.isalnum() else "_" for c in text)

# COMMAND ----------



# COMMAND ----------


