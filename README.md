# bods-generator
BO Data Services Dataflow Generator

This python based program can generate BO Data Services dataflows using CSV as input.

Instructions:
- Create a CSV according to the template.csv format. Provide type (job, workflow, etc), name, parent, child
- Run the program with parameters: 
  1. input csv file (with directory if in another directory)
  2. output xml file
- Load the output xml file into data services
