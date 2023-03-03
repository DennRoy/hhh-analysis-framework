# Script to prepare data set for MVA training
import os, ROOT

from utils import get_scans, mva_variables, luminosities, triggersCorrections,init_mhhh, addMHHH, wps_years

ROOT.gROOT.SetBatch(ROOT.kTRUE)
ROOT.ROOT.EnableImplicitMT()


init_mhhh()

def initialise_df(df,year):
    df = df.Define('triggerSF', triggersCorrections[year][1] )
    cutWeight = '(%f * weight * xsecWeight * l1PreFiringWeight * puWeight * genWeight * triggerSF)'%(lumi)
    df = df.Define('eventWeight',cutWeight)
    df = addMHHH(df)

    wp_loose = wps_years['loose'][year]
    wp_medium = wps_years['medium'][year]
    wp_tight = wps_years['tight'][year]

    count_loose = []
    count_medium = []
    count_tight = []

    for jet in ['jet1','jet2','jet3','jet4','jet5','jet6','jet7','jet8','jet9','jet10']:
        count_loose.append('int(%sDeepFlavB > %f)'%(jet,wp_loose))
        count_medium.append('int(%sDeepFlavB > %f)'%(jet,wp_medium))
        count_tight.append('int(%sDeepFlavB > %f)'%(jet,wp_tight))

    nloose = '+'.join(count_loose)
    nmedium = '+'.join(count_medium)
    ntight = '+'.join(count_tight)


    df = df.Define('nloosebtags',nloose)
    df = df.Define('nmediumbtags',nmedium)
    df = df.Define('ntightbtags',ntight)

    return df

categories = {'nAK8_1' : 'nprobejets == 1',
              'nAK8_1p' : 'nprobejets >= 1',
              'nAK8_2p' : 'nprobejets >= 2',
        }

regions = {'SR' : 'fatJet1Mass > 80 && fatJet1Mass < 150',
           'CR' : 'fatJet1Mass < 80 || fatJet1Mass > 150',
           'inclusive' : 'fatJet1Mass > 0',
        }

pnets = {'loose' : 'fatJet1PNetXbb > 0.95', 
        'medium' : 'fatJet1PNetXbb > 0.975',
        'tight' : 'fatJet1PNetXbb > 0.985',
        }

variables = mva_variables + ['eventWeight']

if __name__ == '__main__':
    year = '2018'

    ROOT.gInterpreter.Declare(triggersCorrections[year][0])
    lumi = luminosities[year]


    version = 'v26'
    #path = '/isilon/data/users/mstamenk/eos-triple-h/v25/mva-inputs-HLT-fit-inputs-all-fixes-v25-inclusive-loose-wp-0ptag-%s'%year
    path = '/isilon/data/users/mstamenk/eos-triple-h/v26/mva-inputs-2018/inclusive/'
    signal_vector = ROOT.std.vector('string')()
    background_vector = ROOT.std.vector('string')()


    for f in ['GluGluToHHHTo6B_SM.root']:
        signal_vector.push_back(path + '/' + f)

    signal_samples = [signal_vector, 'signal']

    for f in ['QCD.root','QCD_bEnriched.root','TT.root','WJetsToQQ.root','WWTo4Q.root','WWW.root','WWZ.root','WZZ.root','ZJetsToQQ.root','ZZTo4Q.root','ZZZ.root']:
    #for f in ['QCD.root','TT.root','WJetsToQQ.root','WWTo4Q.root','WWW.root','WZZ.root','ZJetsToQQ.root','ZZTo4Q.root','ZZZ.root']:
        background_vector.push_back(path + '/' + f)
    background_samples = [background_vector, 'background']

    category = 'nAK8_2p'
    region = 'inclusive'
    pnet = 'loose'

    output = '%s_%s_%s_%s_%s'%(version,year,category,region,pnet)
    if not os.path.isdir(output):
        os.mkdir(output)

    for files, label in [signal_samples,background_samples]:
        print(">>> Extract the training and testing events for {} from the {} dataset.".format(label, files))
        df = ROOT.RDataFrame('Events',files)

        # Category
        
        df = df.Filter(categories[category])
        
        # SR
        df = df.Filter(regions[region])

        # PNet cut for loose
        df = df.Filter(pnets[pnet])

        #  add variables for df
        df = initialise_df(df,year)


        report = df.Report()

        columns = ROOT.std.vector["string"](variables)

        df.Filter("event % 2 == 0", "Select even events for training").Snapshot('Events', output + '/' + 'train_%s.root'%label,columns)
        df.Filter("event % 2 == 1", "Select even events for testing").Snapshot('Events', output + '/' + 'test_%s.root'%label,columns)
        report.Print()


