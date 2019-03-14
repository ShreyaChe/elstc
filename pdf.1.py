from elasticsearch import Elasticsearch
import os
import glob
import PyPDF2
import pandas as pd

os.chdir()
files=glob.glob("*.*")
len(files)
for book in files:
    print(book)
def extractPdffiles(files):
    this_loc = 1 
    df = pd.DataFrame(columns=("name","content"))
    for file in files:
        pdfFileObj = open(file,'rb')
        pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
        n_pages = pdfReader.numPages
        this_doc = ''
        for i in range(n_pages):
            pageObj = pdfReader.getPage(i)
            this_text = pageObj.extractText()
            this_doc +=this_text
        df.loc[this_loc] = file,this_doc
        this_loc = this_loc + 1
    return df
df= extractPdffiles(files)
es=Elasticsearch()
col_names = df.columns
for row_number in range(df.shape[0]):
    body=dict([(name,str(df.iloc[row_number][name])) for name in col_names])
    es.index(index='hadoop',doc_type='books',body=body)
search_results = es.search(index = 'hadoop' ,doc_type = 'books',
    body={"_source":"name",
        'query' : {
            'match_phrase':{"content":"main"},
        }})
search_results['hits']['total']
print(search_results)