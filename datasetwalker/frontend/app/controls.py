control_reference = {}


def add_to_control_reference(key, value):
    global control_reference
    try:
        control_reference[key] = value
    except KeyError as e:
        print(e)
