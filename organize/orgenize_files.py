import os
import sys
import shutil

files_names = []

def organizeFiles(root_folder):
    for subdir, dirs, files in os.walk(root_folder):
        for file in files:
            files_names.append(os.path.join(subdir, file))
    #print(files_names)
    try:
        os.mkdir("./output")
    except OSError:
        print ("Creation of the directory failed")
    else:
        print ("Successfully created the directory" )
    for file in files_names:
        shutil.copy(file, "./output/"+ os.path.basename(os.path.dirname(file))+"." +os.path.basename(file))



if __name__ == "__main__":
    organizeFiles(sys.argv[1])
    