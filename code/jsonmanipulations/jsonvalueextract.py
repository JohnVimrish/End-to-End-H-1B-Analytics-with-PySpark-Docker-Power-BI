from datetime import datetime as dt
import json as js
import configparser


class JsonManupulatorNModifier():

    def __init__(self,
                 file_location_path: str
                 ):

        self.json_file_path = file_location_path

        try:
            with open(self.json_file_path, "r") as confg:
                self.inputjson = js.load(confg)

        except Exception as excep:
            print('Error :', excep)


    def json_val(self, json_key_path, seperator='--'):
        return self.json_value_extractor(json_key_path, seperator)

    def json_value_extractor(self, json_key_path, seperator):

        def json_extractor(parents_key_lst_from_root):
            key_lst = parents_key_lst_from_root
            curr_file = self.inputjson
            curr_json_dict = dict()
            total_parents = len(key_lst) - 1
            # max_len_of_path  = (parents_key_list-1)

            for parent_counter in range(total_parents + 1):

                current_parent = key_lst[parent_counter]
                current_val = curr_file.get(current_parent)

                if type(current_val) is dict:
                    if parent_counter != total_parents:
                        for next_parent_key, next_parent_dict in current_val.items():
                            curr_json_dict[next_parent_key] = next_parent_dict
                        curr_file = curr_json_dict
                    else:
                        return current_val
                else:
                    return current_val

        try:
            json_keys_extracted = list()
            if seperator in json_key_path:
                json_keys_extracted = json_key_path.split(seperator)
            else:
                json_keys_extracted.append(json_key_path)

            return json_extractor(json_keys_extracted)
        except Exception as excep:
            print('Error :', excep)
    
    @staticmethod
    def  write_json_data (json_data,destination) :
        with open(destination, 'w') as out_file:
            js.dump(json_data, out_file)


    @staticmethod
    def  read_config_file (config_file_path:str) :
        config = configparser.ConfigParser()
        config.read(config_file_path)
        return config