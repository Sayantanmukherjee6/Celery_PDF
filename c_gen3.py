# -*- coding: utf-8 -*-
"""
	PDF Scraping with Celery.
	Indentation : Tabs
	To run celery workers : celery -A c_gen3 worker --loglevel=debug
	To run the program : python -m no_c_gen3
	To generate documentation : python -m pydoc -w ./c_gen3.py 
	Source:
		https://www.linode.com/docs/development/python/task-queue-celery-rabbitmq/
		https://tests4geeks.com/python-celery-rabbitmq-tutorial/
"""

from __future__ import absolute_import ####use this to import celery
from tabula import read_pdf
from PyPDF2 import PdfFileReader
from celery import Celery
import numpy as np
import pandas as pd
import re,argparse


# Application name= "c_gen3" (Must be same as program name)
# Backend = Remote Procedural Call
# Broker = RabbitMQ Broker
app = Celery('c_gen3',backend='rpc://',broker='pyamqp://guest@localhost//')


@app.task
def read_save_pdf():
	'''Read pdf, scrap balance sheet table and store it in csv format.'''

	bl_page=0
	Company_Name='AlkaliMetals.pdf'
	pdf = PdfFileReader(open(Company_Name,'rb'))
	count = pdf.getNumPages()

	BS_Meta = ["BALANCE SHEET", "NON-CURRENT ASSETS","Property, plant and equipment","Current liabilities","EQUITY AND LIABILITIES"]

	
	for i in range(0, count):
	    PageObj = pdf.getPage(i)
	    Text = PageObj.extractText()
	    if bl_page==0:
	        if (re.search(BS_Meta[0], Text,re.IGNORECASE) and re.search(BS_Meta[1], Text,re.IGNORECASE) and re.search(BS_Meta[2], Text,re.IGNORECASE)):
	            bl_page=i+1
	            break
	        elif (re.search(BS_Meta[0], Text,re.IGNORECASE) and re.search(BS_Meta[1], Text,re.IGNORECASE)and re.search(BS_Meta[4], Text,re.IGNORECASE)):
	            bl_page=i+1
	            break

	df_bl = read_pdf(Company_Name,pages=bl_page)
	df_bl=df_bl.loc[:, df_bl.isnull().mean() < .8]

	df_bl.to_csv("df_bl.csv",index=False)

if __name__ == "__main__":

	read_save_pdf.delay()
	print ("Thank You")

