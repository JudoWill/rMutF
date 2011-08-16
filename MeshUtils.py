import csv

def convert_mesh_mapping(ifile, ofile):
    
    def record_iter(ifile):
        with open(ifile) as handle:
            assert handle.next().startswith('*N')
            UI = None
            name = None
            for line in handle:
                if line.startswith('*N'):
                    if UI is not None and name is not None:
                        yield UI, name
                    UI = None
                    name = None
                elif line.startswith('MH = '):
                    name = line[5:].strip()
                elif line.startswith('UI = '):
                    UI = line[5:].strip()
    
    with open(ofile, 'w') as handle:
        writer = csv.writer(handle, delimiter = '\t')
        writer.writerows(record_iter(ifile))