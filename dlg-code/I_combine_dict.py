# Updated function
def combine_dicts(static_dict, dynamic_dict):
    combined_parset_dict = static_dict.copy()
    
    # Extracting the dictionary from the list
    dynamic_dict = dynamic_dict[0]
    
    # Adding values from dynamic_dict
    combined_parset_dict["Cimager.dataset"] = dynamic_dict.get('uv_data', '')
    
    # Constructing values for "Cimager.Images.Names" and "Cimager.Images.direction"
    source_name = dynamic_dict.get('name', 'Source')
    fp = dynamic_dict.get('fp', 'A')
    beam_no = dynamic_dict.get('beam_no', 1)
    RA_string = dynamic_dict.get('RA', '')
    Dec_string = dynamic_dict.get('Dec', '')
    
    # Adjusting the values for "Cimager.Images.Names" and "Cimager.Images.direction"
    combined_parset_dict["Cimager.Images.Names"] = [f"image.i.{source_name}.{fp}.B{beam_no}"]
    combined_parset_dict["Cimager.Images.direction"] = [f"{RA_string},{Dec_string}, J2000"]
    
    return combined_parset_dict

def Cimager_test(input_dict):
    for key, value in input_dict.items():
        print(f"{key}: {value}")


# Updated combine dictionary for imcontsub
def combine_dicts_imcontsub(static_dict, dynamic_dict):
    combined_dict = static_dict.copy()

    dynamic_info = dynamic_dict[0]
    name = dynamic_info.get('name', 'Source')
    fp = dynamic_info.get('fp', 'A')
    beam_no = dynamic_info.get('beam_no', 1)

    # Add values from dynamic_dict
    combined_dict["imcontsub.inputfitscube"] = f"image.restored.i.{name}.{fp}.B{beam_no}"
    combined_dict["imcontsub.outputfitscube"] = f"image.restored.i.{name}.{fp}.B{beam_no}.contsub"

    return combined_dict