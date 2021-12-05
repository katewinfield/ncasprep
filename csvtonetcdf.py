import sys
from collections import namedtuple
import pandas as pd
import numpy as np
from netCDF4 import Dataset
from datetime import datetime
import os
import re
import shutil
from shutil import copyfile
import cf
import cfplot as cfp

#function to identify csv files and to organise the directory structure.
def getfile():
    #create an empty list for outfiles
    outfiles = []

    #create an empty list for infiles
    infiles = []

    #dirctort of files to process
    datatxt = '/datacentre/processing3/kate/ozone/data/'

    #for the files listed in the datatxt directory join the path and match the file name
    for files in os.listdir(datatxt):


        filesdir = os.path.join(datatxt, files)

        expression = re.compile('ozone-unit1_(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})_v1.csv')
        matched = expression.match(files)


        if matched:
            #if the file name matches get the year, month and day so we can create directory structure
            year = str(matched.group('year'))
            month = str(matched.group('month'))
            day = str(matched.group('day'))

            yeardir = os.path.join(datatxt, year)
            #check if there is a year directory if not make one and join onto path
            if os.path.isdir(yeardir):
                print('Yes')
            else:
                makeyeardir = os.makedirs(yeardir)

            monthdir = os.path.join(yeardir, month)

            #check if there is a month directory if not make one and join onto path
            if os.path.isdir(monthdir):
                print('Yes')
            else:
                makemonthdir = os.makedirs(monthdir)


            #filepath for renamed files
            in_files = str(
                '/datacentre/processing3/kate/ozone/data/{0}/{1}/ncas-ozone-unit1_{2}{3}{4}_v1.csv'.format(year, month,
                                                                                                        year, month,
                                                                                                         day))
           # datafiles = shutil.move(filesdir, in_files)
            shutil.copy(filesdir, in_files)
            #append the list of files to my infiles list
            infiles.append(in_files)
            print(infiles)

    return infiles

def outfile(infile):
    return infile.replace('.csv', '.nc')


def makenetcdf(filelist):
    for f in filelist:

        #for each file in my filelist create a netcdf4 file
        dataset = Dataset(outfile(f), 'w', format='NETCDF4')

        #define the datatime format
        dateparse = lambda f: datetime.strptime(f, '%d/%m/%Y %H:%M')

        # reads csv files using panda. Notifing there is a header and to skip lines until data. Matching the column names and presenting the datetime.
        df = pd.read_csv(f, header=0, skiprows=[0,1,2,3,4], names=['Time (UTC)', 'Ozone Concentration (ppb)', 'Quality Control Falg Value', 'Quality Control Flag Meaning'], parse_dates = ['Time (UTC)'], date_parser=dateparse)
        print(df)

        #defining the columns for ease
        times = df.get('Time (UTC)')
        print(times)
        ozone = df['Ozone Concentration (ppb)']
        flag = df['Quality Control Falg Value']
        meanings = df['Quality Control Flag Meaning']

        # converts the ozone and flags into numpy arrays. netcdf4-python returns numpy masked arrays. NumPy arrays are smaller memory consumption and better runtime behavior.
        ozonenp = np.array(ozone)
        flagnp = np.array(flag)

        #Creating parts of the netcdf file dimensions and variables
        #The variables store the actual data, the dimensions give the relevant dimension information for the variables, and the attributes provide metadata information about the variables or the dataset itself.
        longitude = dataset.createDimension('longitude')
        latitude = dataset.createDimension('latitude')
        time = dataset.createDimension('time')

        # creating variables
        # time
        timess = dataset.createVariable('times', np.float64, ('time',))
        # converting time to seconds for data standards. It is a standard way of measuring time
        base_time = times[0]
        time_values = []
        #for each time value calculate the time in seconds and append it to the time value list
        for t in times:
            value = t - base_time
            ts = value.total_seconds()
            time_values.append(ts)

        #defining each variable
        timess.units = "seconds since" + base_time.strftime('%Y-%m-%d %H:%M:%S')
        #append the time as numpy array 64 bytes
        timess[:] = np.float64(time_values)

        # ozone
        ozone = dataset.createVariable('Ozone_Concentration', np.float32, ('time',))
        ozone.type = 'float32'
        ozone.units = 'Parts per billion (ppb)'
        ozone.long_name = 'Ozone Concentration'
        ozone.coordinates = 'latitude longitude'
        ozone.cell_methods = 'time:mean'
        ozone.chemical_species = 'O3'
        ozone[:] = np.float32(ozonenp)
        ozone.valid_min = ozone[:][ozone[:]>0].min()
        ozone.valid_max = ozone[:][ozone[:]>0].max()

        ozoneflag = dataset.createVariable('Ozone_flag', np.int8, ('time',))
        ozoneflag.type = 'byte'
        ozoneflag.units = '1'
        ozoneflag.long_name = 'Ozone concentration flag'
        ozoneflag.coordinates = 'latitude longitude'
        ozoneflag.flag_values = '1,2,3,4'
        ozoneflag.flag_meanings = 'Good data' + '\n'
        ozoneflag.flag_meanings = ozoneflag.flag_meanings + 'Missing data' + '\n'
        ozoneflag.flag_meanings = ozoneflag.flag_meanings + 'Data exceeds measurement range' + '\n'
        ozoneflag.flag_meanings = ozoneflag.flag_meanings + 'Measurement below detection threshold'
        ozoneflag[:] = np.int8(flagnp)

        # global attributes
        # read metadata file and add it in
        metadata_file_dir = '/datacentre/processing3/kate/ozone/metadata.xlsx'
        meta = pd.read_excel(metadata_file_dir)
        name = meta.loc[:, 'Name':'Name':1].values
        exp = meta.loc[:, 'Example':'Example':1].values

        for i in range(0, len(name)):
            #for each line convert to numpy array
            msg1 = np.array(name[i])
            msg2 = np.array(exp[i])
            #adding in the attributes
            dataset.setncattr(msg1[0], msg2[0])

        dataset.close()



if __name__ == "__main__":
    # i.e. if file run directly. Runs the functions
    makenetcdf(getfile())

