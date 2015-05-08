#!/usr/bin/env python
import datetime
from glob import glob
import os
import re
from requests import get
import tarfile

from tethys_dataset_services.engines import CkanDatasetEngine

#------------------------------------------------------------------------------
#Main Dataset Manager Class
#------------------------------------------------------------------------------
class CKANDatasetManager(object):
    """
    This class is used to find, zip and upload files to a CKAN data server
    #note: this does not delete the original files
    """
    def __init__(self, engine_url, api_key, model_name, 
                 dataset_notes="CKAN Dataset", 
                 resource_description="CKAN Resource",
                 date_format_string="%Y%m%d"):
        if engine_url.endswith('/'):
            engine_url = engine_url[:-1]
        if not engine_url.endswith('api/action') and not engine_url.endswith('api/3/action'):
            engine_url += '/api/action'
        
        self.dataset_engine = CkanDatasetEngine(endpoint=engine_url, apikey=api_key)
        self.model_name = model_name
        self.dataset_notes = dataset_notes
        self.resource_description = resource_description
        self.date_format_string = date_format_string
        
    def initialize_run(self, watershed, subbasin, date_string):
        """
        Initialize run for watershed upload/download
        """
        self.watershed = watershed
        self.subbasin = subbasin
        self.date_string = date_string
        self.date = datetime.datetime.strptime(self.date_string, self.date_format_string)
        self.dataset_name = '%s-%s-%s-%s-%s' % (self.model_name, 
                                                self.watershed, 
                                                self.subbasin, 
                                                self.date.year, 
                                                self.date.month)
        self.resource_name = '%s-%s-%s-%s' % (self.model_name,
                                             self.watershed, 
                                             self.subbasin,
                                             self.date_string)

    def make_tarfile(self, file_path):
        """
        This function packages the dataset into a tar.gz file and
        returns the path
        """
        base_path = os.path.dirname(file_path)
        output_tar_file =  os.path.join(base_path, "%s.tar.gz" % self.resource_name)
            
        
        if not os.path.exists(output_tar_file):
            with tarfile.open(output_tar_file, "w:gz") as tar:
                    tar.add(file_path, arcname=os.path.basename(file_path))

        return output_tar_file

    def make_directory_tarfile(self, directory_path, search_string="*"):
        """
        This function packages all of the datasets into a tar.gz file and
        returns the path
        """
        base_path = os.path.dirname(directory_path)
        output_tar_file =  os.path.join(base_path, "%s.tar.gz" % self.resource_name)

        if not os.path.exists(output_tar_file):
            directory_files = glob(os.path.join(directory_path,search_string))
            with tarfile.open(output_tar_file, "w:gz") as tar:
                    for directory_file in directory_files:
                        tar.add(directory_file, arcname=os.path.basename(directory_file))

        return output_tar_file
    
    def get_dataset_id(self):
        """
        This function gets the id of a dataset
        """
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = self.dataset_engine.search_datasets({ 'name': self.dataset_name })
        
        if response_dict['success']:
            if int(response_dict['result']['count']) > 0:
                return response_dict['result']['results'][0]['id']
            return None
        else:
            return None

    def create_dataset(self):
        """
        This function creates a dataset if it does not exist
        """
        dataset_id = self.get_dataset_id()
        #check if dataset exists
        if not dataset_id:
            #if it does not exist, create the dataset
            result = self.dataset_engine.create_dataset(name=self.dataset_name,
                                          notes=self.dataset_notes, 
                                          version='1.0', 
                                          tethys_app='erfp_tool', 
                                          waterhsed=self.watershed,
                                          subbasin=self.subbasin,
                                          month=self.date.month,
                                          year=self.date.year)
            dataset_id = result['result']['id']
        return dataset_id
       
    def upload_resource(self, tar_file_path, overwrite=False):
        """
        This function uploads a resource to a dataset if it does not exist
        """
        #create dataset for each watershed-subbasin combo if needed
        dataset_id = self.create_dataset()
        if dataset_id:
            #check if dataset already exists
            
            resource_results = self.dataset_engine.search_resources({'name':self.resource_name},
                                                                    datset_id=dataset_id)
            try:
                if overwrite and resource_results['result']['count'] > 0:
                    #delete resource
                    """
                    CKAN API CURRENTLY DOES NOT WORK FOR UPDATE - bug = needs file or url, 
                    but requres both and to have only one ...

                    #update existing resource
                    print resource_results['result']['results'][0]
                    update_results = self.dataset_engine.update_resource(resource_results['result']['results'][0]['id'], 
                                                        file=file_to_upload,
                                                        url="",
                                                        date_uploaded=datetime.datetime.utcnow().strftime("%Y%m%d%H%M"))
                    """
                    self.dataset_engine.delete_resource(resource_results['result']['results'][0]['id'])

                if resource_results['result']['count'] <=0 or overwrite:
                    
                    #upload resources to the dataset
                    self.dataset_engine.create_resource(dataset_id, 
                                                    name=self.resource_name, 
                                                    file=tar_file_path,
                                                    format='tar.gz', 
                                                    tethys_app="erfp_tool",
                                                    watershed=self.watershed,
                                                    subbasin=self.subbasin,
                                                    forecast_date=self.date_string,
                                                    description=self.resource_description)
                else:
                    print "Resource exists. Skipping ..."
            except Exception,e:
                print e
                pass
         
    def zip_upload_file(self, file_path):
        """
        This function uploads a resource to a dataset if it does not exist
        """
        #zip file and get dataset information
        print "Zipping files for watershed: %s %s" % (self.watershed, self.subbasin)
        tar_file_path = self.make_tarfile(file_path)    
        print "Finished zipping files"
        print "Uploading datasets"
        self.upload_resource(tar_file_path)
        os.remove(tar_file_path)
        print "Finished uploading datasets"

    def zip_upload_directory(self, directory_path, search_string="*", overwrite=False):
        """
        This function uploads a resource to a dataset if it does not exist
        """
        #zip file and get dataset information
        print "Zipping files for watershed: %s %s" % (self.watershed, self.subbasin)
        tar_file_path = self.make_directory_tarfile(directory_path, search_string)    
        print "Finished zipping files"
        print "Uploading datasets"
        self.upload_resource(tar_file_path, overwrite)
        os.remove(tar_file_path)
        print "Finished uploading datasets"
           
    def get_resource_info(self):
        """
        This function gets the info of a resource
        """
        dataset_id = self.get_dataset_id()
        if dataset_id:
            #check if dataset already exists
            resource_results = self.dataset_engine.search_resources({'name': self.resource_name},
                                                                    datset_id=dataset_id)
            try:
                if resource_results['result']['count'] > 0:
                    #upload resources to the dataset
                    return resource_results['result']['results'][0]
            except Exception,e:
                print e
                pass
        return None
    
    def download_resource(self, extract_directory):
        """
        This function downloads a resource
        """
        resource_info = self.get_resource_info()
        if resource_info:
            #only download if file does not exist already
            if not os.path.exists(extract_directory):
                resource_url = resource_info['url']
                print "Downloading files for watershed:", self.watershed, self.subbasin
                try:
                    os.makedirs(extract_directory)
                except OSError:
                    pass

                local_tar_file = "%s.tar.gz" % self.resource_name
                local_tar_file_path = os.path.join(extract_directory,
                                                   local_tar_file)
                r = get(resource_url, stream=True)
                with open(local_tar_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024): 
                        if chunk: # filter out keep-alive new chunks
                            f.write(chunk)
                            f.flush()

                print "Extracting file(s)"
                tar = tarfile.open(local_tar_file_path)
                tar.extractall(extract_directory)
                tar.close()
                os.remove(local_tar_file_path)
                print "Finished downloading and extracting file(s)"
                return True
            else:
                print "Resource exists locally. Skipping ..."
                return True
        else:
            print "Resource not found in CKAN. Skipping ..."
            return False

    def download_prediction_resource(self, watershed, subbasin, date_string, extract_directory):
        """
        This function downloads a prediction resource
        """
        self.initialize_run(watershed, subbasin, date_string)
        self.download_resource(extract_directory)

#------------------------------------------------------------------------------
#ECMWF RAPID Dataset Manager Class
#------------------------------------------------------------------------------
class ECMWFRAPIDDatasetManager(CKANDatasetManager):
    """
    This class is used to find and download, zip and upload ECMWFRAPID 
    prediction files from/to a data server
    """
    def __init__(self, engine_url, api_key):
        super(ECMWFRAPIDDatasetManager, self).__init__(engine_url, 
                                                        api_key,
                                                        'erfp',
                                                        "ECMWF-RAPID Flood Predicition Dataset",
                                                        'This dataset contians NetCDF3 files produced by '
                                                        'downscalsing ECMWF forecasts and routing them with RAPID',
                                                        "%Y%m%d.%H"
                                                        )
    def initialize_run_ecmwf(self, watershed, subbasin, date_string):
        """
        Initialize run for watershed upload/download custom for ecmwf
        """
        self.initialize_run(watershed, subbasin, date_string[:11])
        self.date_string = date_string
    
    def get_subbasin_name_list(self, source_directory, subbasin_name_search):
        """
        Get a list of subbasins in directory
        """
        subbasin_list = []
        outflow_files = sorted(glob(os.path.join(source_directory,'Qout_*.nc')))
        for outflow_file in outflow_files:
            subbasin_name = subbasin_name_search.search(os.path.basename(outflow_file)).group(1)
            if subbasin_name not in subbasin_list:
                subbasin_list.append(subbasin_name)
        return subbasin_list
        
    def zip_upload_resources(self, source_directory):
        """
        This function packages all of the datasets in to tar.gz files and
        returns their attributes
        """
        watersheds = [d for d in os.listdir(source_directory) \
                        if os.path.isdir(os.path.join(source_directory, d))]
        subbasin_name_search = re.compile(r'Qout_(\w+)_[a-zA-Z\d]+.nc')

        for watershed in watersheds:
            watershed_dir = os.path.join(source_directory, watershed)
            date_strings = [d for d in os.listdir(watershed_dir) \
                            if os.path.isdir(os.path.join(watershed_dir, d))]
            for date_string in date_strings:
                subbasin_list = self.get_subbasin_name_list(os.path.join(watershed_dir, date_string), 
                                                       subbasin_name_search)
                for subbasin in subbasin_list:
                    self.initialize_run_ecmwf(watershed, subbasin, date_string)
                    self.zip_upload_directory(os.path.join(watershed_dir, date_string), 
                                              'Qout_%s*.nc' % subbasin)

    def download_recent_resource(self, watershed, subbasin, main_extract_directory):
        """
        This function downloads the most recent resource within 3 days
        """
        iteration = 0
        download_file = False
        today_datetime = datetime.datetime.utcnow()
        #search for datasets within the last 3 days
        while not download_file and iteration < 6:
            days, hours = divmod(iteration*12,24)
            today =  today_datetime - datetime.timedelta(days, hours)
            hour = '1200' if today.hour > 11 else '0'
            date_string = '%s.%s' % (today.strftime("%Y%m%d"), hour)
            
            self.initialize_run_ecmwf(watershed, subbasin, date_string)
            resource_info = self.get_resource_info()
            if resource_info and main_extract_directory and os.path.exists(main_extract_directory):
                extract_directory = os.path.join(main_extract_directory, self.watershed, date_string)
                download_file = self.download_resource(extract_directory)
            iteration += 1
                    
        if not download_file:
            print "Recent resources not found. Skipping ..."
                                      
#------------------------------------------------------------------------------
#WRF-Hydro RAPID Dataset Manager Class
#------------------------------------------------------------------------------
class WRFHydroHRRRDatasetManager(CKANDatasetManager):
    """
    This class is used to find and download, zip and upload ECMWFRAPID 
    prediction files from/to a data server
    """
    def __init__(self, engine_url, api_key):
        super(WRFHydroHRRRDatasetManager, self).__init__(engine_url, 
                                                        api_key,
                                                        'wrfp',
                                                        "WRF-Hydro HRRR Flood Predicition Dataset",
                                                        'This dataset contians NetCDF3 files produced by '
                                                        'downscalsing WRF-Hydro forecasts and routing them with RAPID',
                                                        "%Y%m%dT%H%MZ"
                                                        )
          
    def zip_upload_resource(self, source_file, watershed, subbasin):
        """
        This function packages all of the datasets in to tar.gz files and
        returns their attributes
        """
        #WRF-Hydro HRRR time format string "%Y%m%dT%H%MZ"
        file_name = os.path.basename(source_file)
        date_string = file_name.split("_")[1]
        self.initialize_run(watershed, subbasin, date_string)
        self.zip_upload_file(source_file)


#------------------------------------------------------------------------------
#RAPID Input Dataset Manager Class
#------------------------------------------------------------------------------
class RAPIDInputDatasetManager(CKANDatasetManager):
    """
    This class is used to find and download, zip and upload ECMWFRAPID 
    prediction files from/to a data server
    """
    def __init__(self, engine_url, api_key, model_name, app_instance_id):
        self.app_instance_id = app_instance_id
        super(RAPIDInputDatasetManager, self).__init__(engine_url, 
                                                        api_key,
                                                        model_name,
                                                        "RAPID Input Dataset for %s" % model_name,
                                                        'This dataset contians RAPID files for %s' % model_name)

    def initialize_run(self, watershed, subbasin):
        """
        Initialize run for watershed upload/download
        """
        self.watershed = watershed
        self.subbasin = subbasin
        self.date = datetime.datetime.utcnow()
        self.date_string = self.date.strftime(self.date_format_string)
        self.dataset_name = '%s-rapid-input-%s' % (self.model_name, self.app_instance_id)
        self.resource_name = '%s-%s-%s-rapid-input' % (self.model_name,
                                                       self.watershed, 
                                                       self.subbasin)
    def zip_upload_resource(self, source_directory):
        """
        This function adds RAPID files in to zip files and
        uploads files to data store
        """
        #get info for waterhseds
        basin_name_search = re.compile(r'rapid_namelist_(\w+).dat')
        namelist_file = glob(os.path.join(source_directory,'rapid_namelist_*.dat'))[0]
        subbasin = basin_name_search.search(namelist_file).group(1)
        watershed = os.path.basename(source_directory)
     
        self.initialize_run(watershed, subbasin)
        self.zip_upload_directory(source_directory)

    def download_model_resource(self, watershed, subbasin, extract_directory):
        """
        This function downloads a prediction resource
        """
        self.initialize_run(watershed, subbasin)
        self.download_resource(extract_directory)


if __name__ == "__main__":
    """    
    Tests for the datasets
    """
    engine_url = 'http://ciwckan.chpc.utah.edu'
    api_key = '8dcc1b34-0e09-4ddc-8356-df4a24e5be87'
    #ECMWF
    """
    er_manager = ECMWFRAPIDDatasetManager(engine_url, api_key)
    er_manager.zip_upload_resources(source_directory='/home/alan/work/rapid/output/')
    er_manager.download_prediction_resource(watershed='magdalena', 
                                            subbasin='el_banco', 
                                            date_string='20150505.0', 
                                            extract_directory='/home/alan/work/rapid/output/magdalena/20150505.0')
    """
    #WRF-Hydro
    """
    wr_manager = WRFHydroHRRRDatasetManager(engine_url, api_key)
    wr_manager.zip_upload_resource(source_file='/home/alan/Downloads/RapidResult_20150405T2300Z_CF.nc',
                                    watershed='usa',
                                    subbasin='usa')
    wr_manager.download_prediction_resource(watershed='usa', 
                                            subbasin='usa', 
                                            date_string='20150405T2300Z', 
                                            extract_directory='/home/alan/tethysdev/tethysapp-erfp_tool/wrf_hydro_rapid_predictions/usa')
    """
    #RAPID Input
    """
    app_instance_id = '53ab91374b7155b0a64f0efcd706854e'
    ri_manager = RAPIDInputDatasetManager(engine_url, api_key, 'ecmwf', app_instance_id)
    ri_manager.zip_upload_resource(source_directory='/home/alan/work/tmp_input/nfie_texas_gulf_region')
    ri_manager.download_model_resource(watershed='nfie_texas_gulf_region', 
                                       subbasin='huc_2_12', 
                                       extract_directory='/home/alan/work/tmp_input/nfie_texas_gulf_region')
    """
