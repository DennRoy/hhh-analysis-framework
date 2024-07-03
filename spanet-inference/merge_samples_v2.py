import os, glob



# argument parser
import argparse
parser = argparse.ArgumentParser(description='Args')
parser.add_argument('-v','--version', default='v28-QCD-modelling') # version of NanoNN production
parser.add_argument('--year', default='2018') # year
parser.add_argument('--typename', default='categorisation-spanet-boosted-classification') # typename
parser.add_argument('--regime', default='inclusive-weights') # regime
parser.add_argument('--inpath',default = '/users/mstamenk/scratch/mstamenk')
parser.add_argument('--outpath',default = '/users/mstamenk/scratch/mstamenk/eos-triple-h')
parser.add_argument('--outsize',default = 1.0) # Maximum output filesize in GB
args = parser.parse_args()

year = args.year
version = args.version
typename = args.typename
regime = args.regime
outsize = args.outsize * 1024 # MB

isRemote = False
if args.inpath.startswith("/store"):
    isRemote = True
    import subprocess
    if "dmroy" in args.inpath:
        prefix = 'davs://cmsxrootd.hep.wisc.edu:1094/'
    else:
        import socket
        host = socket.getfqdn()
        if 'cern.ch' in host:
            prefix = 'root://xrootd-cms.infn.it//'
        else:
            prefix = 'root://cmseos.fnal.gov//'
    def GetFromGfal(command):
        #print(">>> "+command)
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)
        out = ""
        for line in iter(process.stdout.readline,b""):
            if isinstance(line,bytes):
                line = line.decode('utf-8')
            out += line
        process.stdout.close()
        retcode = process.wait()
        out = out.strip()
        return out

samples = {}
sizes = {}

if not isRemote:
    path = os.path.join(args.inpath, version, 'mva-inputs-%s-%s'%(year,typename), regime)
    files = glob.glob(os.path.join(path, '*.root'))

    for f in files:
        sample = os.path.basename(f)
        splits = sample.split('_tree_')
        sample_type = splits[0]
        if sample_type not in samples:
            samples[sample_type] = []
            sizes[sample_type] = []
        samples[sample_type].append(f)
        sizes[sample_type].append(float(os.path.getsize(f))/1024/1024) # MB

    output_path = os.path.join(args.outpath, '%s-merged-selection'%version, 'mva-inputs-%s-%s'%(year,typename), regime)
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

else:
    path = os.path.join(prefix+args.inpath, version, 'mva-inputs-%s-%s'%(year,typename), regime)
    files = GetFromGfal("(eval $(scram unsetenv -sh); gfal-ls {path} -l)".format(path=path)).split("\n")
    files = {l.split()[8]:float(l.split()[4])/1024/1024 for l in files}

    for f in files:
        sample = f
        splits = sample.split('_tree_')
        sample_type = splits[0]
        if sample_type not in samples:
            samples[sample_type] = []
            sizes[sample_type] = []
        samples[sample_type].append(os.path.join(path, f))
        sizes[sample_type].append(files[f]) # MB

    output_path = os.path.join(prefix+args.outpath, '%s-merged-selection'%version, 'mva-inputs-%s-%s'%(year,typename), regime)
    cmd = "(eval $(scram unsetenv -sh); gfal-mkdir {output} -p)".format(output=output_path)
    process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
    log = process.communicate()[0]

#print(samples)
if isRemote:
    DoneRemoteFiles = GetFromGfal("(eval $(scram unsetenv -sh); gfal-ls {path})".format(path=output_path)).split("\n")

for sample in samples:
    fullsize = sum(sizes[sample])
    noutputs = int(fullsize / outsize)+1

    i = 0
    part = 0
    while i<len(samples[sample]):
        tohadd = []
        tohadd.append(i)
        i+=1
        while i<len(samples[sample]):
            if sum([sizes[sample][j] for j in tohadd]) + sizes[sample][i] < outsize:
                tohadd.append(i)
                i+=1
            else:
                break
        part+=1
        partstr = "_part"+str(part)
        if part==1 and i==len(samples[sample]): partstr=""
        if not isRemote:
            if os.path.isfile(os.path.join(output_path, sample+partstr+".root")): continue
            cmd = f'hadd -f -j -n 0 {os.path.join(output_path, sample+partstr+".root")} {" ".join([samples[sample][j] for j in tohadd])}'
            print(">>> "+cmd)
            os.system(cmd)
        else:
            if sample+partstr+".root" in DoneRemoteFiles: continue
            tmp_path = os.getenv("TMPDIR")
            for s in [samples[sample][j] for j in tohadd]:
                cmd = "(eval $(scram unsetenv -sh); gfal-copy {f_in} {tmpdir})".format(f_in=s, tmpdir=tmp_path)
                process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
                log = process.communicate()[0]
            cmd = f'hadd -f -j -n 0 {os.path.join(tmp_path, sample+partstr+".root")} {" ".join([os.path.join(tmp_path, os.path.basename(samples[sample][j])) for j in tohadd])}'
            print(">>> "+cmd)
            os.system(cmd)
            cmd = "(eval $(scram unsetenv -sh); gfal-copy {tmpdir} {f_out})".format(tmpdir=os.path.join(tmp_path, sample+partstr+".root"), f_out=output_path)
            process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,shell=True)
            log = process.communicate()[0]
            for s in [samples[sample][j] for j in tohadd]:
                os.remove(os.path.join(tmp_path, os.path.basename(s)))
            os.remove(os.path.join(tmp_path, sample+partstr+".root"))
