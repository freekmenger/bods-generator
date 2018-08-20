# bods-generator
BO Data Services Dataflow Generator

This python based program can generate BO Data Services dataflows using CSV as input. The current version only supports generating separat jobs, workflows and dataflows. 

Instructions:
- Create a CSV according to the sample.csv format. Provide type (job, workflow or dataflow), name and subobjects (optional)
  Make sure objects are given hierarchical with the lowest object first - if they are dependent.
  E.g. workflow that is involved in job: first line in the csv is the workflow, second line is the job
- Run the program with parameters: 
  1. input csv file (with directory if in another directory)
  2. output xml file (with directory if in another directory)
- Load the output xml file into data services

Example:
python codeGenerator.py sample.csv output.xml

Tested against BODS version:
14.2.10.1748
In theory it should work with other versions of BODS

