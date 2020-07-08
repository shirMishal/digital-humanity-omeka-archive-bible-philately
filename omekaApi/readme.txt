Code is useful to edit columns in csv file contained refrence to another "omeka resource" using omeka API.

running instruction:
1. generate key_identity and key_credential with your omeka acount
   more info: https://omeka.org/s/docs/user-manual/admin/users/#api-key
2. update var install_location = "http://site_name.reclaim_hoasting_domain"
3. structures and terms are competible with templates at "resource template" folder
4. run: python ./richdataapi.py ./path_to_csv_input_file

example for inputs and outputs available in folders: refs, quotes, philately