def get_type(s):
    try:
        float(s)
        if '.' not in s:
            return 'INT NOT NULL'
    except ValueError:
        return 'VARCHAR(100)'