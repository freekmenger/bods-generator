import sys
import csv
import pdb
from lxml import etree

def read_csv(i_filename):
    ''' 
    i_filename: the directory and filename of the csv file (semicolon separated) that will be read
    Header row will be ignored. File structure: type, name, parent, source, target
    Types:
    - job: has name, comma separated sub-objects (opt)
    - workflow: has name, comma separated sub-objects (opt)
    - dataflow: has name, source (opt), target (opt)
    - transformation: has name, parent, source, target
    Returns all the BodsObjects in a list
    '''
    l_BodsObjects = []
    with open(i_filename, 'r') as csvfile:
        objectreader = csv.reader(csvfile, delimiter=';', quotechar='|')
        l_rowcounter = 1
        for row in objectreader:
            if l_rowcounter != 1:
                l_BodsObjects.append(row)
            l_rowcounter += 1
    return(l_BodsObjects)


def generate_output(i_BodsObjects, i_output):
    ''' 
    i_BodsObjects: a list of BodsObjects that should be included in the xml
    i_output: the directory and filename of the xml file that will be created
    Returns message for generating the xml file
    '''
    # create XML 
    root = etree.Element('DataIntegratorExport', 
                             repositoryVersion='14.2.10.0000', productVersion='14.2.10.1748')
        # Make a new document tree
    doc = etree.ElementTree(root)
    #loop through each object from the csv, then add to the XML tree
    for object in i_BodsObjects:
        doc = object.genXML(root, doc)
    outFile = open(i_output, 'wb')
    doc.write(outFile, xml_declaration=True, encoding='UTF-8', pretty_print=True) 


class BodsObject(object):
    ''' 
    BodsObject: a BodsObject that can be of different types (job, workflow, dataflow)
    '''
    def __init__(self, type, name, subobjects):
        #create a BodsObject with type type
        self.type = type
        self.name = name
        self.subobjects = subobjects
    def getType(self):
        #return the type
        return self.type
    def getName(self):
        #return the name
        return self.name
    def getSubobjects(self):
        #return the subobjects
        return self.subobjects
    def genXML(self, root, doc):
        ''' 
        root - root xml document (etree module)
        doc - document element tree under the root xml document (etree module)
        Returns doc - add the job xml objects to the xml tree
        '''
        if self.type == 'job':
            doc = self.genXMLjob(root, doc)
        elif self.type == 'workflow':
            doc = self.genXMLworkflow(root, doc)            
        elif self.type == 'dataflow':
            doc = self.genXMLdataflow(root, doc)
        return (doc)
    def genXMLjob(self, root, doc):
        ''' 
        root - root xml document (etree module)
        doc - document element tree under the root xml document (etree module)
        Returns doc - add the job xml objects to the xml tree
        '''
        DIJob= etree.SubElement(root, 'DIJob', name=self.name, typeId='2')
        DISteps = etree.SubElement(DIJob, 'DISteps')
        if len(self.subobjects) > 0:
            for subObject in self.subobjects.split(','):
                DICallStep = etree.SubElement(DISteps, 'DICallStep', typeId="0", calledObjectType="Workflow", name=subObject)
        DIAttributes = etree.SubElement(DIJob, 'DIAttributes')
        DIAttribute1 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_checkpoint_enabled", value="no")
        DIAttribute2 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_collect_statistics", value="no")
        DIAttribute3 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_collect_statistics_monitor", value="no")
        DIAttribute4 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_create_debug_package", value="no")
        DIAttribute5 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_enable_assemblers", value="yes")
        DIAttribute6 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_enable_audit", value="yes")
        DIAttribute7 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_enable_dataquality", value="yes")
        DIAttribute8 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_export_repo", value="no")
        DIAttribute9 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_export_reports", value="no")
        DIAttribute10 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_isrecoverable", value="no")
        DIAttribute11 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_mode", value="Multi-Process")
        DIAttribute12 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_monitor_sample_rate", value="1000")
        DIAttribute13 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_monitor_timer_rate", value="5")
        DIAttribute14 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_name", value="export")
        DIAttribute15 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_print_version", value="no")
        DIAttribute16 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_testmode_enabled", value="no")
        DIAttribute17 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_abapquery", value="no")
        DIAttribute18 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_all", value="no")
        DIAttribute19 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_ascomm", value="no")
        DIAttribute20 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_assemblers", value="no")
        DIAttribute21 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_audit", value="no")
        DIAttribute22 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_dataflow", value="yes")
        DIAttribute23 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_idoc_file", value="no")
        DIAttribute24 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_memory_loader", value="no")
        DIAttribute25 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_memory_reader", value="no")
        DIAttribute26 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_optimized_dataflow", value="no")
        DIAttribute27 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_parallel_execution", value="no")
        DIAttribute28 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_rfc_function", value="no")
        DIAttribute29 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_row", value="no")
        DIAttribute30 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_script", value="no")
        DIAttribute31 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_session", value="yes")
        DIAttribute32 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_sql_only", value="no")
        DIAttribute33 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_sqlfunctions", value="no")
        DIAttribute34 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_sqlloaders", value="no")
        DIAttribute35 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_sqlreaders", value="no")
        DIAttribute36 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_sqltransforms", value="no")
        DIAttribute37 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_stored_procedure", value="no")
        DIAttribute38 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_table", value="no")
        DIAttribute39 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_table_reader", value="no")
        DIAttribute40 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_transform", value="no")
        DIAttribute41 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_userfunction", value="no")
        DIAttribute42 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_usertransform", value="no")
        DIAttribute43 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_trace_workflow", value="yes")
        DIAttribute44 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_type", value="batch")
        DIAttribute45 = etree.SubElement(DIAttributes, 'DIAttribute', name="job_use_statistics", value="yes")
        DIAttribute46 = etree.SubElement(DIAttributes, 'DIAttribute', name="locale_codepage", value="&lt;default&gt;")
        DIAttribute47 = etree.SubElement(DIAttributes, 'DIAttribute', name="locale_language", value="&lt;default&gt;")
        DIAttribute48 = etree.SubElement(DIAttributes, 'DIAttribute', name="locale_territory", value="&lt;default&gt;")
        return doc
    def genXMLworkflow(self, root, doc):
        '''
        root - root xml document (etree module)
        doc - document element tree under the root xml document (etree module)
        Returns doc - add the workflow xml objects to the xml tree
        '''
        DIWorkflow= etree.SubElement(root, 'DIWorkflow', name=self.name, typeId='2')
        DISteps = etree.SubElement(DIWorkflow, 'DISteps')
        if len(self.subobjects) > 0:
            for subObject in self.subobjects.split(','):
                DICallStep = etree.SubElement(DISteps, 'DICallStep', typeId="1", calledObjectType="Dataflow", name=subObject)
        DIAttributes = etree.SubElement(DIWorkflow, 'DIAttributes')
        DIAttribute1 = etree.SubElement(DIAttributes, 'DIAttribute', name="run_once", value="no")
        DIAttribute2 = etree.SubElement(DIAttributes, 'DIAttribute', name="unit_of_recovery", value="no")
        DIAttribute3 = etree.SubElement(DIAttributes, 'DIAttribute', name="workflow_type", value="Regular")
        return doc
    def genXMLdataflow(self, root, doc):
        ''' 
        root - root xml document (etree module)
        doc - document element tree under the root xml document (etree module) 
        Returns doc - add the dataflow xml objects to the xml tree
        '''        
        DIDataflow = etree.SubElement(root, 'DIDataflow', 
                                          name=self.name,
                                          typeId='1')
        DIAttributes = etree.SubElement(DIDataflow, 'DIAttributes')
        DIAttribute1 = etree.SubElement(DIAttributes, 'DIAttribute', name="Cache_type", value="pageable_cache")
        DIAttribute2 = etree.SubElement(DIAttributes, 'DIAttribute', name="Parallelism_degree", value="default")
        DIAttribute3 = etree.SubElement(DIAttributes, 'DIAttribute', name="allows_both_input_and_output", value="yes")
        DIAttribute4 = etree.SubElement(DIAttributes, 'DIAttribute', name="dataflow_loader_bulk_load", value="no")
        DIAttribute5 = etree.SubElement(DIAttributes, 'DIAttribute', name="dataflow_loader_commit_size", value="default")
        DIAttribute6 = etree.SubElement(DIAttributes, 'DIAttribute', name="dataflow_loader_row_size_bytes", value="0")
        DIAttribute7 = etree.SubElement(DIAttributes, 'DIAttribute', name="dataflow_number_of_loaders", value="0")
        DIAttribute8 = etree.SubElement(DIAttributes, 'DIAttribute', name="run_once", value="no")
        DIAttribute9 = etree.SubElement(DIAttributes, 'DIAttribute', name="use_dataflow_links", value="no")
        DIAttribute10 = etree.SubElement(DIAttributes, 'DIAttribute', name="use_datastore_links", value="yes")
        DIAttribute11 = etree.SubElement(DIAttributes, 'DIAttribute', name="validation_xform_exists", value="no")
        DIAttribute12 = etree.SubElement(DIAttributes, 'DIAttribute', name="validation_xform_stats", value="no")
        return doc
        

if __name__ == '__main__':
    assert len(sys.argv) >= 3
    l_filename = str(sys.argv[1])
    l_bObjects = read_csv(l_filename)
    l_BOlist = []
    for object in l_bObjects:
        l_BOlist.append(BodsObject(object[0],object[1],object[2]))
        #Do specific generation task associated with each object type
        #E.g. if dataflow, first the tables should be generated (separate BodsObjects)
    i_output = str(sys.argv[2])
    generate_output(l_BOlist, i_output)
    
    