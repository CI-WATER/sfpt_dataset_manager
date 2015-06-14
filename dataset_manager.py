#!/usr/bin/env python
import datetime
from glob import glob
import os
import re
from requests import get
from shutil import rmtree
import tarfile
import zipfile

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
        self.watershed = watershed.lower()
        self.subbasin = subbasin.lower()
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
       
    def upload_resource(self, file_path, overwrite=False, file_format='tar.gz'):
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
                #determine if results are exact or similar
                same_ckan_resource_id = ""
                if resource_results['result']['count'] > 0:
                    for resource in resource_results['result']['results']:
                        if resource['name'] == self.resource_name:
                            same_ckan_resource_id = resource['id']
                            break
                        
                if overwrite and same_ckan_resource_id:
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
                    self.dataset_engine.delete_resource(same_ckan_resource_id)

                if not same_ckan_resource_id or overwrite:
                    
                    #upload resources to the dataset
                    return self.dataset_engine.create_resource(dataset_id, 
                                                    name=self.resource_name, 
                                                    file=file_path,
                                                    format=file_format, 
                                                    tethys_app="erfp_tool",
                                                    watershed=self.watershed,
                                                    subbasin=self.subbasin,
                                                    forecast_date=self.date_string,
                                                    description=self.resource_description)
                                                    
                else:
                    print "Resource", self.resource_name ,"exists. Skipping ..."
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
        resource_info = self.upload_resource(tar_file_path)
        os.remove(tar_file_path)
        print "Finished uploading datasets"
        return resource_info

    def zip_upload_directory(self, directory_path, search_string="*", overwrite=False):
        """
        This function uploads a resource to a dataset if it does not exist
        """
        #zip file and get dataset information
        print "Zipping files for watershed: %s %s" % (self.watershed, self.subbasin)
        tar_file_path = self.make_directory_tarfile(directory_path, search_string)    
        print "Finished zipping files"
        print "Uploading datasets"
        resource_info = self.upload_resource(tar_file_path, overwrite)
        os.remove(tar_file_path)
        print "Finished uploading datasets"
        return resource_info
           
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
                    for resource in resource_results['result']['results']:
                        if resource['name'] == self.resource_name:
                            #upload resources to the dataset
                            return resource
            except Exception,e:
                print e
                pass
        return None

    def get_dataset_info(self):
        """
        This function gets the info of a resource
        """
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = self.dataset_engine.search_datasets({ 'name': self.dataset_name })
        
        if response_dict['success']:
            if int(response_dict['result']['count']) > 0:
                for dataset in response_dict['result']['results']:
                    if dataset['name'] == self.dataset_name:
                        #upload resources to the dataset
                        return dataset
            return None
        else:
            return None

    
    def download_resource_from_info(self, extract_directory, resource_info_array):
        """
        Downloads a resource from url
        """
        #only download if file does not exist already
        if not os.path.exists(extract_directory):
            print "Downloading and extracting files for watershed:", self.watershed, self.subbasin
            try:
                os.makedirs(extract_directory)
            except OSError:
                pass
            for resource_info in resource_info_array:
                #for resource
                file_format = resource_info['format']
                
                local_tar_file = "%s.%s" % (resource_info['name'], file_format)
                local_tar_file_path = os.path.join(extract_directory,
                                                   local_tar_file)
                r = get(resource_info['url'], stream=True)
                with open(local_tar_file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024): 
                        if chunk: # filter out keep-alive new chunks
                            f.write(chunk)
                            f.flush()
    
                if file_format.lower() == "tar.gz":
                    with tarfile.open(local_tar_file_path) as tar:
                        tar.extractall(extract_directory)
                elif file_format.lower() == "zip":
                    with zipfile.ZipFile(local_tar_file_path) as zip_file:
                        zip_file.extractall(extract_directory)
                else:
                    print "Unsupported file format. Skipping."

                os.remove(local_tar_file_path)
                
            print "Finished downloading and extracting file(s)"
            return True
        else:
            print "Resource exists locally. Skipping ..."
            return False

    def download_resource(self, extract_directory):
        """
        This function downloads a resource
        """
        resource_info = self.get_resource_info()
        if resource_info:
            self.download_resource_from_info(extract_directory, 
                                             [resource_info])
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
        self.watershed = watershed.lower()
        self.subbasin = subbasin.lower()
        self.date_string = date_string[:11]
        self.date = datetime.datetime.strptime(self.date_string, self.date_format_string)
        self.dataset_name = '%s-%s-%s-%s' % (self.model_name, 
                                             self.watershed, 
                                             self.subbasin, 
                                             self.date.strftime("%Y%m%dt%H"))
                                                
    def update_resource_ensemble_number(self, ensemble_number):
        """
        Set ensemble number in resource name for ecmwf resource
        """
        self.resource_name = '%s-%s-%s-%s-%s' % (self.model_name,
                                                 self.watershed, 
                                                 self.subbasin,
                                                 self.date_string,
                                                 ensemble_number)
    
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

    def zip_upload_directory(self, directory_path, search_string="*"):
        """
        This function packages all of the datasets into individual tar.gz files and
        uploads them to the dataset
        """
        base_path = os.path.dirname(directory_path)
        ensemble_number_search = re.compile(r'Qout_\w+_(\d+).nc')

        #zip file and get dataset information
        print "Zipping and uploading files for watershed: %s %s" % (self.watershed, self.subbasin)
        directory_files = glob(os.path.join(directory_path,search_string))
        for directory_file in directory_files:
            ensemble_number = ensemble_number_search.search(os.path.basename(directory_file)).group(1)
            self.update_resource_ensemble_number(ensemble_number)
            #tar.gz file
            output_tar_file =  os.path.join(base_path, "%s.tar.gz" % self.resource_name)
            if not os.path.exists(output_tar_file):
                with tarfile.open(output_tar_file, "w:gz") as tar:
                    tar.add(directory_file, arcname=os.path.basename(directory_file))
            #upload file
            resource_info = self.upload_resource(output_tar_file)
            os.remove(output_tar_file)
        print "%s datasets uploaded" % len(directory_files)
        return resource_info

        
    def zip_upload_resources(self, source_directory):
        """
        This function packages all of the datasets in to tar.gz files and
        returns their attributes
        """
        watersheds = [d for d in os.listdir(source_directory) \
                        if os.path.isdir(os.path.join(source_directory, d))]
        subbasin_name_search = re.compile(r'Qout_(\w+)_\d+.nc')

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
        This function downloads the most recent resource within 6 days
        """
        iteration = 0
        download_file = False
        today_datetime = datetime.datetime.utcnow()
        #search for datasets within the last 6 days
        while not download_file and iteration < 12:
            today =  today_datetime - datetime.timedelta(seconds=iteration*12*60*60)
            hour = '1200' if today.hour > 11 else '0'
            date_string = '%s.%s' % (today.strftime("%Y%m%d"), hour)
            
            self.initialize_run_ecmwf(watershed, subbasin, date_string)
            #get list of all resources
            dataset_info = self.get_dataset_info()
            if dataset_info and main_extract_directory and os.path.exists(main_extract_directory):
                #make sure there are at least 52 or at lest a day has passed before downloading
                if dataset_info['num_resources'] >= 52 or (today_datetime-today >= datetime.timedelta(1)):
                    extract_directory = os.path.join(main_extract_directory, self.watershed, self.subbasin, date_string)
                    download_file = self.download_resource_from_info(extract_directory,
                                                     dataset_info['resources'])

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


    def download_recent_resource(self, watershed, subbasin, main_extract_directory):
        """
        This function downloads the most recent resource within 1 day
        """
        iteration = 0
        download_file = False
        today_datetime = datetime.datetime.utcnow()
        #search for datasets within the last day
        while not download_file and iteration < 24:
            today =  today_datetime - datetime.timedelta(seconds=iteration*60*60)
            
            date_string = today.strftime(self.date_format_string)
            self.initialize_run(watershed, subbasin, date_string)
            resource_info = self.get_resource_info()
            if resource_info and main_extract_directory and os.path.exists(main_extract_directory):
                extract_directory = os.path.join(main_extract_directory, self.watershed, self.subbasin)
                download_file = self.download_resource(extract_directory)
            iteration += 1
                    
        if not download_file:
            print "Recent resources not found. Skipping ..."

#------------------------------------------------------------------------------
#RAPID Input Dataset Manager Class
#------------------------------------------------------------------------------
class RAPIDInputDatasetManager(CKANDatasetManager):
    """
    This class is used to find and download, zip and upload ECMWFRAPID 
    prediction files from/to a data server
    """
    def __init__(self, engine_url, api_key, model_name, app_instance_id):
        super(RAPIDInputDatasetManager, self).__init__(engine_url, 
                                                        api_key,
                                                        model_name,
                                                        "RAPID Input Dataset for %s" % model_name,
                                                        'This dataset contians RAPID files for %s' % model_name)
        self.app_instance_id = app_instance_id
        self.dataset_name = '%s-rapid-input-%s' % (self.model_name, self.app_instance_id)

    def initialize_run(self, watershed, subbasin):
        """
        Initialize run for watershed upload/download
        """
        self.watershed = watershed.lower()
        self.subbasin = subbasin.lower()
        self.date = datetime.datetime.utcnow()
        self.date_string = self.date.strftime(self.date_format_string)
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
        namelist_files = glob(os.path.join(source_directory,'rapid_namelist_*.dat'))
        if namelist_files:
            subbasin = basin_name_search.search(namelist_files[0]).group(1)
            watershed = os.path.basename(source_directory)
         
            self.initialize_run(watershed, subbasin)
            self.zip_upload_directory(source_directory)

    def download_model_resource(self, resource_info, extract_directory):
        """
        This function downloads a prediction resource
        """
        self.initialize_run(resource_info['watershed'], resource_info['subbasin'])
        self.download_resource_from_info(extract_directory, [resource_info])

    def upload_model_resource(self, upload_file, watershed, subbasin):
        """
        This function uploads file to CKAN
        """
        self.initialize_run(watershed, subbasin)
        resource_info = self.upload_resource(upload_file, 
                                             True,
                                             '.zip')
        os.remove(upload_file)
        return resource_info
        
    def sync_dataset(self, extract_directory):
        """
        This function syncs the dataset with the directory
        """
        # Use the json module to load CKAN's response into a dictionary.
        dataset_info = self.get_dataset_info()
        if dataset_info:
            #get list of resources on CKAN
            current_ckan_resources = [d for d in dataset_info['resources'] \
                                      if 'watershed' in d and 'subbasin' in d]

            #get list of watersheds and subbasins on local instance
            current_local_resources = []
            subbasin_name_search = re.compile(r'rapid_namelist_(\w+).dat')
            watersheds = [d for d in os.listdir(extract_directory) \
                          if os.path.isdir(os.path.join(extract_directory, d))]
    
            for watershed in watersheds:
                namelist_files = glob(os.path.join(extract_directory, watershed, 'rapid_namelist_*.dat'))
                if not namelist_files:
                    current_local_resources.append({'watershed': watershed, 'subbasin': ""})
                for namelist_file in namelist_files:
                    subbasin = subbasin_name_search.search(namelist_file).group(1)
                    current_local_resources.append({'watershed': watershed, 'subbasin': subbasin})


            date_compare = datetime.datetime.utcnow()-datetime.timedelta(hours=12, minutes=30)
            #STEP 1: Remove resources no longer on CKAN or update local resource
            for local_resource in current_local_resources:
                ckan_resource = [d for d in current_ckan_resources if \
                                    (d['watershed'].lower() == local_resource['watershed'].lower()) and \
                                    d['subbasin'].lower() == local_resource['subbasin'].lower()]
                if not ckan_resource:
                    #remove resources no longer on CKAN
                    print "LOCAL DELETE", local_resource['watershed'], local_resource['subbasin']
                    rmtree(os.path.join(extract_directory, local_resource['watershed']))
                elif datetime.datetime.strptime(ckan_resource[0]['created'].split(".")[0], "%Y-%m-%dT%H:%M:%S") > date_compare:
                    #2015-05-12T14:01:08.572338
                    #remove out of date local resources
                    print "LOCAL PAST DELETE", local_resource['watershed'], local_resource['subbasin']
                    rmtree(os.path.join(extract_directory, local_resource['watershed']))
            
            #STEP 2: Add new resources to local instance if not already here
            for ckan_resource in current_ckan_resources:         
                print "ATTEMPT DOWNLOAD RESOURCE", ckan_resource['watershed'], ckan_resource['subbasin']
                self.download_model_resource(ckan_resource,
                                             os.path.join(extract_directory, ckan_resource['watershed']))


if __name__ == "__main__":
    """    
    Tests for the datasets
    """
    #engine_url = 'http://ciwckan.chpc.utah.edu'
    #api_key = '8dcc1b34-0e09-4ddc-8356-df4a24e5be87'
    #ECMWF
    """
    er_manager = ECMWFRAPIDDatasetManager(engine_url, api_key)
    er_manager.zip_upload_resources(source_directory='/home/alan/work/rapid/output/')
    er_manager.download_prediction_resource(watershed='magdalena', 
                                            subbasin='el_banco', 
                                            date_string='20150505.0', 
                                            extract_directory='/home/alan/work/rapid/output/magdalena/20150505.0')
    er_manager.download_recent_resource(watershed="rio_yds", 
                                        subbasin="palo_alto", 
                                        main_extract_directory='/home/alan/tethysdev/tethysapp-erfp_tool/ecmwf_rapid_predictions' )
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
                                            extract_directory='/home/alan/tethysdev/tethysapp-erfp_tool/wrf_hydro_rapid_predictions/usa/usa')
    wr_manager.download_recent_resource(watershed='usa', 
                                        subbasin='usa', 
                                        main_extract_directory='/home/alan/tethysdev/tethysapp-erfp_tool/wrf_hydro_rapid_predictions/usa/usa')
    """
    #RAPID Input
    """
    app_instance_id = 'eb76561dc4ba513c994a00f7721becf1'
    ri_manager = RAPIDInputDatasetManager(engine_url, api_key, 'ecmwf', app_instance_id)
    ri_manager.zip_upload_resource(source_directory='/home/alan/work/tmp_input/rio_yds')
    ri_manager.download_model_resource(watershed='nfie_texas_gulf_region', 
                                       subbasin='huc_2_12', 
                                       extract_directory='/home/alan/work/tmp_input/nfie_texas_gulf_region')
    ri_manager.sync_dataset('/home/alan/work/tmp_input/')
    """
