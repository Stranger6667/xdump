import zipfile

import pytest


@pytest.mark.usefixtures("schema", "data")
def test_single_full_table(cli, archive_filename, db_helper):
    result = cli.dump("-f", "groups")
    assert not result.exception
    assert result.output == "Dumping ...\nOutput file: {0}\nDone!\n".format(archive_filename)
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)


@pytest.mark.usefixtures("schema", "data")
def test_multiple_full_tables(cli, archive_filename, db_helper):
    result = cli.dump("-f", "groups", "-f" "tickets")
    assert not result.exception
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_groups(archive)
    db_helper.assert_content(
        archive,
        "tickets",
        {
            b"id,author_id,subject,message",
            b"1,1,Sub 1,Message 1",
            b"2,2,Sub 2,Message 2",
            b"3,2,Sub 3,Message 3",
            b"4,2,Sub 4,Message 4",
            b"5,3,Sub 5,Message 5",
        },
    )


@pytest.mark.usefixtures("schema", "data")
def test_partial_tables(cli, archive_filename, db_helper):
    result = cli.dump("-p", "employees:SELECT * FROM employees WHERE id = 1")
    assert not result.exception
    archive = zipfile.ZipFile(archive_filename)
    db_helper.assert_content(archive, "groups", {b"id,name", b"1,Admin"})
    db_helper.assert_content(
        archive,
        "employees",
        {
            b"id,first_name,last_name,manager_id,referrer_id,group_id",
            b"1,John,Doe,,,1",
        },
    )


@pytest.mark.usefixtures("schema", "data")
def test_partial_tables_invalid(cli):
    result = cli.dump("-p", "shit")
    assert result.exception
    assert result.output.endswith(
        'Invalid value for "-p" / "--partial": partial table specification should be in '
        'the following format: "table:select SQL"\n'
    )


@pytest.mark.usefixtures("schema", "data")
def test_no_schema(cli, archive_filename):
    result = cli.dump("-f", "groups", "--no-schema")
    assert not result.exception
    archive = zipfile.ZipFile(archive_filename)
    assert archive.namelist() == ["dump/data/groups.csv"]
