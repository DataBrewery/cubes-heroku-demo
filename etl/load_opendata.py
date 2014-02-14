import subprocess
import os
import json
from collections import OrderedDict
from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, Column, Integer, String, Numeric
import csv

engine = create_engine("sqlite:///data/opendata.sqlite")
metadata = MetaData(bind=engine)

GIT = "git"
SOURCE_DATA = "data/source"

FIELD_TYPES = {
    "integer": (Integer, int),
    "string": (String, str),
    "number": (Numeric, float)
}

sources = [
    {
        "name": "employment-us",
        "url": "https://github.com/Stiivi/employment-us",
        "dataset": "aat1.csv"
    },
    {
        "name": "gdp",
        "url": "https://github.com/Stiivi/gdp",
        "dataset": "gdp.csv"
    }
]

def download_package(name, url):
    repo = os.path.join(SOURCE_DATA, name)
    if os.path.exists(repo):
        cwd = os.getcwd()
        os.chdir(repo)
        subprocess.call(["git", "pull"])
        os.chdir(cwd)
    else:
        cwd = os.getcwd()
        os.chdir(SOURCE_DATA)
        subprocess.call(["git", "clone", url, name])
        os.chdir(cwd)


class DataPackageResource(object):
    def __init__(self, package, resource):
        self.package = package
        path = resource.get("path")
        if not path:
            raise Exception("Resources without local path are not supported")

        self.name = resource["name"]
        self.title = resource.get("title")
        self.path = os.path.join(package.path, path)
        self.fields = resource["schema"]["fields"]


class DataPackage(object):
    def __init__(self, path):
        self.path = path

        infopath = os.path.join(path, "datapackage.json")
        with open(infopath) as f:
            try:
                metadata = json.load(f)
            except Exception as e:
                raise Exception("Unable to read %s: %s"
                                % (infopath, str(e)))

        self.name = metadata.get("name")
        self.resources = OrderedDict()
        for res in metadata["resources"]:
            try:
                self.resources[res["name"]] = DataPackageResource(self, res)
            except Exception as e:
                print "WARNING: Ignoring resource %s: %s" % (res["name"], str(e))

    def resource(self, name):
        return self.resources[name]


def load_resource(table_name, resource):
    table = Table(table_name, metadata, autoload=False)
    if table.exists():
        table.drop(engine)
    table = Table(table_name, metadata, autoload=False)

    for field in resource.fields:
        type_ = FIELD_TYPES[field["type"]][0]
        column = Column(field["name"], type_)
        table.append_column(column)

    metadata.create_all(engine)

    insert = table.insert()

    converters = [FIELD_TYPES[f["type"]][1] for f in resource.fields]
    names = [f["name"] for f in resource.fields]

    with open(resource.path) as f:
        reader = csv.reader(f)
        reader.next()
        for row in reader:
            values = [conv(value) for value, conv in zip(row, converters)]
            record = dict(zip(names, values))
            engine.execute(insert.values(record))


def load_all():

    if not os.path.exists(SOURCE_DATA):
        os.makedirs(SOURCE_DATA)

    # Download packages
    for info in sources:
        print info["name"]
        download_package(info["name"], info["url"])

    packages = []
    for info in sources:
        path = os.path.join(SOURCE_DATA, info["name"])
        package = DataPackage(path)
        packages.append(package)

    for package in packages:
        print "Processing package %s..." % package.name
        for resource in package.resources.values():
            print "    Loading resource %s..." % resource.name
            load_resource(resource.name, resource)


load_all()
