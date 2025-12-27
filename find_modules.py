import pkgutil
import aspose.email

def find_submodules():
    print(f"--- Exploring submodules of 'aspose.email' ---")
    path = aspose.email.__path__
    for importer, modname, ispkg in pkgutil.walk_packages(path=path, prefix=aspose.email.__name__+'.'):
        print('Found submodule %s ' % modname)

if __name__ == "__main__":
    find_submodules()
