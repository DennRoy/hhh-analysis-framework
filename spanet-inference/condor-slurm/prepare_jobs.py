# Scritp to prepare jobs for plotting with make_histograms_rdataframe.py

import os, glob  
import ROOT
import math
import cppyy

# Hardcoded inputs:
#years = ['2018']
#version = 'v31-merged-selection-no-lhe'
#regime = 'inclusive-weights'
#mainpath = '/users/mstamenk/scratch/mstamenk'
# Mine:
#years = ['2022']
#version = 'v2022-parts-no-lhe'
#regime = 'inclusive-weights'
#mainpath = '/eos/user/d/dmroy/HHH/sampleProduction/TestOutput/'

import argparse
parser = argparse.ArgumentParser(description='Args')
parser.add_argument('-v','--version', default='v31-merged-selection-no-lhe')
parser.add_argument('--year', default='2018')
parser.add_argument('--regime', default='inclusive-weights')
parser.add_argument('--path', default='/users/mstamenk/scratch/mstamenk')
args = parser.parse_args()

years = [args.year]
version = args.version
regime = args.regime
mainpath = args.path

# Consistent with default paths in both scripts:
scripts = {0: 'predict_spanet_classification_pnet_all_vars.py', 1: 'predict_spanet_classification_categorisation.py'}
inputdir = 'mva-inputs-%s'
outputdir1 = 'mva-inputs-%s-spanet-boosted-classification'
outputdir2 = 'mva-inputs-%s-categorisation-spanet-boosted-classification'

isRemote = False
if mainpath.startswith("/store"):
    isRemote = True
    import subprocess
    if "dmroy" in mainpath:
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


scripts = {i: os.path.join(os.environ["CMSSW_BASE"], 'src/hhh-analysis-framework/spanet-inference', scripts[i]) for i in scripts}
filedirs = {0: inputdir, 1: outputdir1, 2: outputdir2}
pwd = os.path.join(os.environ["CMSSW_BASE"], 'src/hhh-analysis-framework/spanet-inference', 'condor-slurm')
jobs_path = 'jobs'

#submit="Universe   = vanilla\nrequest_memory = 7900\nExecutable = %s\nArguments  = $(ClusterId) $(ProcId)\nLog        = log/job_%s_%s_%s.log\nOutput     = output/job_%s_%s_%s.out\nError      = error/job_%s_%s_%s.error\nQueue 1"
submit="Universe   = vanilla\nrequest_memory = 2000\nExecutable = %s\nArguments  = $(ClusterId) $(ProcId)\nLog        = log/job_%s_%s_%s.log\nOutput     = output/job_%s_%s_%s.out\nError      = error/job_%s_%s_%s.error\nQueue 1"
submit_lxplus='universe              = vanilla\nexecutable            = %s\narguments             = $(ProcId)\nlog                   = log/job_%s_$(ClusterId).log\noutput                = output/job_%s_$(ClusterId).$(ProcId).out\nerror                 = error/job_%s_$(ClusterId).$(ProcId).err\ngetenv                = true\n\n+JobFlavour = "microcentury"\nRequestCpus = 4\n\nqueue %s'
job_cmd = '#! /bin/bash\n#SBATCH -p batch\n#SBATCH -n 1\n#SBATCH -t 03:00:00\n#SBATCH --mem=24g\n%s\nexit'
if not isRemote:
    job_cmd_lxplus = '#!/bin/bash\nsource /cvmfs/cms.cern.ch/cmsset_default.sh\nfunction peval { echo ">>> $@"; eval "$@"; }\nWORKDIR="$PWD"\nif [ ! -z "$CMSSW_BASE" -a -d "$CMSSW_BASE/src" ]; then\n  peval "cd $CMSSW_BASE/src"\n  peval "eval `scramv1 runtime -sh`"\n  peval "cd $WORKDIR"\nfi\nsource /cvmfs/sft.cern.ch/lcg/views/setupViews.sh LCG_104 x86_64-el9-gcc11-opt\n\n'
else:
    job_cmd_lxplus = '#!/bin/bash\nsource /cvmfs/cms.cern.ch/cmsset_default.sh\nfunction peval { echo ">>> $@"; eval "$@"; }\nWORKDIR="$PWD"\nif [ ! -z "$CMSSW_BASE" -a -d "$CMSSW_BASE/src" ]; then\n  peval "cd $CMSSW_BASE/src"\n  peval "eval `scramv1 runtime -sh`"\n  peval "cd $WORKDIR"\nfi\n\n'
#job_cmd = '%s'


submit_all = 'submit_all.sh'
submit_all_lxplus = 'submit_all_lxplus.sh'
manual_all = 'manual_run_all.sh'

jobs = []
jobs_lxp = []
manual_jobs = []


def WriteFile(path, content):
    print("Writing %s"%os.path.join(jobs_path, os.path.basename(path)))
    with open(path, 'w') as f:
        f.write(content)

batch_size = 50e3
for year in years:
    if not isRemote:
        #path_to_samples = '/users/mstamenk/scratch/mstamenk//samples-%s-%s-nanoaod/'%(version,year)
        #path_to_samples = '/users/mstamenk/scratch/mstamenk//%s/mva-inputs-%s/%s/'%(version,year,regime)
        #path_to_samples = '/eos/user/d/dmroy/HHH/sampleProduction/TestOutput/mva-inputs-%s/%s/'%(year,regime)
        path_to_samples = os.path.join(mainpath, version, inputdir%(year), regime)
        files = glob.glob(os.path.join(path_to_samples, '*.root'))
        #files = [f for f in files if 'TTToSemi' in f or 'W' in f or 'Z' in f]
        samples = [os.path.basename(s).replace('.root','') for s in files]
    else:
        path_to_samples = os.path.join(prefix+mainpath, version, inputdir%(year), regime)
        files = GetFromGfal("(eval $(scram unsetenv -sh); gfal-ls {path})".format(path=path_to_samples)).split()
        samples = [f.replace('.root','') for f in files if f.endswith(".root")]
        files = [os.path.join(path_to_samples, f) for f in files if f.endswith(".root")]
    jobcount = 0
    job_cmd_lxplus_full = job_cmd_lxplus
    print(samples)
    print(files)
    RemoteOutputs = {}
    for i in range(len(files)):
        f_in = samples[i]
        f = files[i]
        df = ROOT.RDataFrame("Events", f)
        entries = df.Count().GetValue()
        n_batches = math.floor(float(entries)/batch_size)
        for j in range(n_batches+1):
            # Check if previous jobs completed
            scriptl = []
            for k in list(reversed([0,1])):
                expoutfile = f.replace(inputdir%(year), filedirs[k+1]%(year)).replace('.root', '_%d.root'%j)
                print(expoutfile)
                if isRemote:
                    if os.path.dirname(expoutfile) not in RemoteOutputs:
                        RemoteOutputs[os.path.dirname(expoutfile)] = GetFromGfal("(eval $(scram unsetenv -sh); gfal-ls {path})".format(path=os.path.dirname(expoutfile))).split()
                    if os.path.basename(expoutfile) in RemoteOutputs[os.path.dirname(expoutfile)]:
                        break # Checking files individually always stalls infinitely at some point, so don't do it
                        try:
                            df = ROOT.TFile.Open(expoutfile, "open")
                            #tree = df.Get("Events")
                            #entries = tree.GetEntries()
                            #if entries > 0: break # If output file exists and has valid content, move on
                            if not df.IsZombie(): break
                        except cppyy.gbl.std.runtime_error:
                            pass
                elif os.path.isfile(expoutfile):
                    try:
                        df = ROOT.RDataFrame("Events", expoutfile)
                        entries = df.Count().GetValue()
                        if entries > 0: break # If output file exists and has valid content, move on
                    except cppyy.gbl.std.runtime_error:
                        pass
                scriptl.append(scripts[k])
                #break
            if scriptl==[]: continue
            scriptl = list(reversed(scriptl))

            filename = 'job_%s_%s_%d.sh'%(f_in,year,j)
            cmds = []
            for i,script in enumerate(scriptl):
              if not isRemote:
                  cmd = 'python3 %s --f_in %s --year %s --batch_size %d --batch_number %d --version %s --regime %s --path %s'%(script,f_in,year,batch_size,j,version,regime,mainpath)
                  print(cmd)
              else:
                  src = ''
                  if script.endswith("predict_spanet_classification_pnet_all_vars.py"):
                      src = 'mva-inputs-%s'%(year)
                      dst = 'mva-inputs-%s-spanet-boosted-classification'%(year)
                  elif script.endswith("predict_spanet_classification_categorisation.py"):
                      src = 'mva-inputs-%s-spanet-boosted-classification'%(year)
                      dst = 'mva-inputs-%s-categorisation-spanet-boosted-classification'%(year)
                  assert(src!='')
                  if i==0:
                      cmd = '(eval $(scram unsetenv -sh); gfal-copy %s $TMPDIR/%s -p)'%(os.path.join(prefix+mainpath, version, src, regime, f_in+".root"), os.path.join(version, src, regime, f_in+".root"))
                      cmds.append(cmd)
                  cmd = '(source /cvmfs/sft.cern.ch/lcg/views/setupViews.sh LCG_104 x86_64-el9-gcc11-opt; python3 %s --f_in %s --year %s --batch_size %d --batch_number %d --version %s --regime %s --path %s)'%(script,f_in,year,batch_size,j,version,regime,"$TMPDIR")
                  print(cmd)
                  cmds.append(cmd)
                  cmd = '(eval $(scram unsetenv -sh); gfal-copy $TMPDIR/%s %s -p)'%(os.path.join(version, dst, regime, f_in+"_"+str(j)+".root"), os.path.join(prefix+mainpath, version, dst, regime, f_in+"_"+str(j)+".root"))
              cmds.append(cmd)
            cmd = "; ".join(cmds)

            WriteFile(os.path.join(jobs_path, filename), job_cmd%cmd)
            manual_jobs.append(filename)

            job_cmd_lxplus_full += 'if [ $1 -eq %d ]; then\n  %s\nfi\n\n'%(jobcount,cmd)
            jobcount += 1

            submit_file = 'submit_%s_%s_%s'%(f_in,year,j)
            WriteFile(os.path.join(jobs_path, submit_file), submit%(os.path.join(jobs_path,filename),f_in,year,j,f_in,year,j,f_in,year,j))
            jobs.append(submit_file)

    if jobcount>0:
        filename_lxp = 'job_%s_lxplus.sh'%(year)
        WriteFile(os.path.join(jobs_path, filename_lxp), job_cmd_lxplus_full)

        submit_file_lxp = 'submit_%s_lxplus.sub'%(year)
        WriteFile(os.path.join(jobs_path, submit_file_lxp), submit_lxplus%(os.path.join(jobs_path,filename_lxp),year,year,year,jobcount))
        jobs_lxp.append(submit_file_lxp)
        print("SUBMIT WITH: source submit_all_lxplus.sh")
    else:
        print("ALL JOBS DONE FOR",year,"!")

cmd = '#!/bin/bash\n'
for j in jobs:
    cmd += 'condor_submit %s \n'%(os.path.join(jobs_path, j))

with open(submit_all, 'w') as f:
    f.write(cmd)

cmd = '#!/bin/bash\n'
for j in jobs_lxp:
    cmd += 'condor_submit %s \n'%(os.path.join(jobs_path, j))

with open(submit_all_lxplus, 'w') as f:
    f.write(cmd)

if len(manual_jobs) > 1000:
    cmd1 = '#!/bin/bash\n'
    for f in manual_jobs[:999]:
        cmd1+= 'sbatch %s\n'%(os.path.join(pwd,jobs_path,f))
    with open(manual_all,'w') as f:
        f.write(cmd1)

    cmd2 =  '#!/bin/bash\n'
    for f in manual_jobs[999:]:
        cmd2+= 'sbatch %s\n'%(os.path.join(pwd,jobs_path,f))
    with open(manual_all.replace('.sh','_2.sh'), 'w') as f:
        f.write(cmd2)

else:
    cmd = '#!/bin/bash\n'
    for f in manual_jobs:
        cmd += 'sbatch %s\n'%(os.path.join(pwd,jobs_path,f))

    with open(manual_all,'w') as f:
        f.write(cmd)
                        
