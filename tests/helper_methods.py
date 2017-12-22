import mock


def open_output(return_file):
    file_mock = mock.mock_open().return_value
    file_mock.__enter__.return_value = return_file
    return file_mock


def create_open_return(out):
    return mock.mock_open().return_value if out is None else open_output(out)


def create_mock_context_manager(outputs):
    open_mock = mock.mock_open()
    open_mock.side_effect = map(create_open_return, outputs)
    return open_mock
