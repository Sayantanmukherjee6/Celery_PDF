# -*- coding: utf-8 -*-
"""
	PDF Scraping without Celery.
	Indentation : Tabs
	TO run the program : python -m no_c_gen3
	To generate documentation : python -m pydoc -w ./no_c_gen3.py 
"""

from tabula import read_pdf
from PyPDF2 import PdfFileReader
import numpy as np
import pandas as pd
import re,argparse



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

	read_save_pdf()
	print ("Thank You")

