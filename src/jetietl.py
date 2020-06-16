import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
import os

import googlespreadsheethandler as gxlsx

import configparser

#GH = googlespreadsheethandler.SpreadsheetHandler(os.path.join(secdict,"credentials.json"),os.path.join(secdict,"token.pickle"))
#            field = self.config.get(section,'file_fields')
##            file_id=self.config.get(section,'file_id')
#            df=GH.read(file_id,field)


def loadconfig(filename):
    config = configparser.ConfigParser()
    config.read(filename)
    config_check(config)
    return config

def config_check(config):
    path=config.get("GoogleSecret","path_of_secret")
    assert len(path)>0


def load_orig_data(config):
    secdir = config.get("GoogleSecret", "path_of_secret")
    GH = gxlsx.SpreadsheetHandler(os.path.join(secdir, "credentials.json"),
                                                     os.path.join(secdir, "token.pickle"))
    section="WorklogSource"
    field = config.get(section, 'file_fields')
    file_id = config.get(section, 'file_id')
    df = GH.read(file_id, field)

    new_col_name=config.get(section,'fieldnames').split(",")
    real_names=[n.strip() for n in new_col_name]
    df.columns=real_names
    df = df[df.iloc[:,0]!=""].copy()
    return df

def run_etl(config):
    df = load_orig_data(config)
    print(len(df))





if __name__=="__main__":
    print("Run JetiETL")

    if len(sys.argv)!=2:
        print("ERROR - Missing config filename argument!")
        exit()
    else:
        configfilename = str(sys.argv[-1])
        print("Loading config file: {}".format(configfilename))
        config = loadconfig(configfilename)

    run_etl(config)