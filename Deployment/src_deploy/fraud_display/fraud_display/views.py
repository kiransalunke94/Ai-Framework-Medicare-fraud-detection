#!/usr/bin/env python
# coding: utf-8

# In[ ]:

#from django.shortcuts import render
#import requests

#def button(request):
 #   return render(request,'home.html')
#def output(request):
 #   data=requests.get("https://www.google.com")
  #  print(data.text)
  #  data=data.text
    
 #   context = {'data':data}
    
    
  #  return render(request, 'home.html', context)

from django.http import HttpResponse
import pandas
def read_file(request):
   # return render(request,'home.html')
#def output(request):
    f=open('/home/kiran/medicare/data/output/np.txt', 'r')
    #f=pandas.read_csv("C:\\Users\\Nikhil\\Weddings.csv")
    file_content= f.read()
    f.close()
    #context={'file_content':file_content}
    return HttpResponse(file_content,content_type="text/plain")
