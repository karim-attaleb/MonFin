import csv
import os
from pathlib import Path, PurePath
from os.path import basename
from glob import glob
import tkinter as tk
from tkinter import filedialog as fd
import shutil
import zipfile

import openpyxl

import helper
import sqlite3


def get_files(dir):
    files = [f.path for f in os.scandir(dir)]
    return files

# too slow
# def get_files_ext(dir, ext, recur= "False"):
#     files = glob(dir + "\\*." + ext, recursive=recur)
#     return files


# sneller manier om door dir recursief te gaan
def walk(path, ext):
    files = []
    count = 0
    for p, d, f in os.walk(path):
        for file in f:
            if file.endswith(ext):
                files.append(os.path.join(p, file))
                # files.append(file)
                count +=1
    # print(files)
    helper.logger.info("Number of %s files read from %s: %s", ext, path, count)
    return files


def left(s, amount):
    return s[:amount]


def right(s, amount):
    return s[-amount:]


def mid(s, offset, amount):
    return s[offset:offset+amount]


# for dirpath, subdirs, files in os.walk(path):
#     for x in files:
#         if x.endswith(".shp"):
#             shpfiles.append(os.path.join(dirpath, x))


def check_folder(path):
    is_exist = os.path.exists(path)
    if not is_exist:
        os.makedirs(path)
        helper.logger.info("The new directory is created!")


def choose_files(dir):
    root = tk.Tk()
    root.title('Kies de file(s) die je wilt verwerken')
    filetypes = (('zip file', '*.zip'),
                 ('All files', '*.*'))
    # filenames = fd.askopenfilenames(title = 'Open file(s)', initialdir= os.getcwd(), filetypes=filetypes)
    filenames = fd.askopenfilenames(title='Open file(s)', initialdir=dir, filetypes=filetypes)
    #showinfo(title='Geselecteerde files', message=filenames) #toon gevonden file(s) in messagebox
    root.destroy()
    helper.logger.info("Get file(s) from user: %s", list(filenames))
    return list(filenames) #returns list ipv tuple


def copy_file(src, dst):
    shutil.copyfile(src, dst)
    # shutil.copy2(src, dst) #to preserve timestamp


def get_filename(fullpath):
    file = Path(fullpath)
    return file.name


def get_path(full_path):
    return Path(full_path).parent


def get_filename_without_extension(filename):
    return PurePath(filename).stem


def get_filename_extension(filename):
    return PurePath(filename).suffix


def join_path_filename(path, filename):
    return PurePath(path).joinpath(filename)


def remove_file(file):
    del_file = Path(file)
    del_file.unlink()


def remove_dir_and_files(full_path): #recursief
    path = Path(full_path)
    for child in path.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            remove_dir_and_files(child)
    path.rmdir()


# https://gist.github.com/msunardi/6527ac4c3b08975d30f83cd8aa80e147
def extract_files_by_ext_from_zip(zip_file, ext, output_path):
    count_files = 0
    with zipfile.ZipFile(zip_file, 'r') as archive:
        list_files = archive.namelist()
        for file_name in list_files:
            if file_name.endswith(ext):
                count_files += 1
                archive.extract(file_name, output_path)


def get_all_file_paths(directory):
    file_paths = []
    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename) # join the two strings in order to form the full filepath.
            file_paths.append(filepath)
    return file_paths


def zip_multiple_files(source, target_filename): #full_path wordt in zip weergegeven
    directory = source # path to folder which needs to be zipped
    file_paths = get_all_file_paths(directory) # calling function to get all file paths in the directory

    with zipfile.ZipFile(target_filename, 'w') as zip: # writing files to a zipfile
        for file in file_paths: # writing each file one by one
            zip.write(file)
        helper.logger.info('All files zipped successfully!')


def zip_multiple_files_flatten(source, target_filename):
        with zipfile.ZipFile(target_filename, 'w') as archive:
            for root, directories, files in os.walk(source):
                for filename in files:
                    filePath = os.path.join(root, filename) # create complete filepath of file in directory
                    archive.write(filePath, basename(filePath)) # Add file to zip


def compress_directory(source_dir, output_filename, kind='zip'):
    helper.logger.info("Zipping folder %s: ", source_dir)
    shutil.make_archive(output_filename, kind, source_dir)


def extract_all_files_from_zip(zip_file, output_path):
    helper.logger.info("Starting full extraction of zipfile %s", zip_file)
    count_files = 0

    with zipfile.ZipFile(zip_file, 'r') as zipObject:
        list_filenames = zipObject.namelist()
        for fileName in list_filenames:
            count_files = count_files + 1
            zipObject.extract(fileName, output_path)
    helper.logger.info("Finished full extraction - %s - extractions", count_files)

def correct_file(full_path, old, new):
    with open(full_path, "r") as infile:
        filedata = infile.read()

    filedata = filedata.replace(old, new)

    with open(full_path, "w") as outfile:
        outfile.write(filedata)


def convert_csv_to_excel(full_path):
    helper.logger.info("Convert csv file to excel")
    path = get_path(full_path)
    old_file = get_filename(full_path)
    new_file = get_filename_without_extension(old_file) + ".xlsx"
    new_path = os.path.join(path, new_file)
    # print(old_file)
    # print(new_file)

    wb = openpyxl.Workbook()
    ws = wb.active
    with open(full_path) as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            ws.append(row)
    wb.save(new_path)
    helper.logger.info("Finished convert csv file to excel")



# def get_connection_db(mydbfile):
#     db_conn = sqlite3.connect(mydbfile)
#     c = db_conn.cursor()
#     return db_conn, c