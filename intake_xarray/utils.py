def rev_format(format_string, resolved_text):
    from string import Formatter

    fmt = Formatter()
    args = {}
    _text = resolved_text

    for i in fmt.parse(format_string):
        literal_text, field_name, format_spec, conversion = i
        _text = _text.replace(literal_text, '', 1)

        if field_name is not None:
            if not format_spec:
                raise ValueError('Format spec missing for {}.'.format(field_name))

            args[field_name] = fmt.format_field(_text, format_spec)
            _text = _text.replace(args[field_name], '', 1)

    return args

def pattern_to_glob(format_string):
    from string import Formatter

    fmt = Formatter()

    fields = {i[1]: '*' for i in fmt.parse(format_string)}
    return format_string.format(**fields)
