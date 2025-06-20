def detect_encoding(file_path, sample_size=1024):
    encodings = ['utf-8', 'iso-8859-1', 'windows-1252', 'utf-16']
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(sample_size)
            return encoding
        except UnicodeDecodeError:
            continue
    return 'iso-8859-1'