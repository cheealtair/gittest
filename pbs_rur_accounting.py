#!/usr/bin/python

'''
(C) Copyright 2016 Altair Engineering, Inc. All rights reserved.
This code is provided "as is" without any warranty, express or implied, or
indemnification of any kind.  All other terms and conditions are as
specified in the Altair PBS EULA.

Author: Clinton Chee
Based on: Scott Suchyta, Shwetha Shetty, Avinaba Roy

Requirements:
- Cray's Resource Utilization Report (RUR) must be installed and enabled on the node hosting pbs_mom
- Altair's RUR plugin must be installed. This plugin will ensure that the RUR files are written into 
- a location accessible by the PBS MOM hook (PBS_HOME/spool) so that it can extract the necessary metrics.
- PBS MOM hook only understands Cray's RUR json-dict output.
- RUR plugins, taskstats, energy, timestamp and memory module(s) are being incorporated into the PBS Professional accounting logs.

Assumptions:
- Terminology: plugin = Cray RUR plugin such as memory, taskstats. Metric = the various variable captured for that plugin. 
  For example, the memory plugin has one metric called current_freemem.
- Standard checks like: PBS_HOME environment variable , pbs.conf file , resourcedef (file) entries and the rur.<job_id>
  file in the /var/spool/PBS/spool/ are conducted. In order to ensure that resources are populated completely by the
  hook program it's vital that access and validity of the path(s) mentioned are accurate.
- Metrics from the RUR plugins are _not_ expected to clobber native PBS Professional resources. 
- Some RUR generated metrics' value(s) are in the form of a flat dictionary or an array. These will be treated at each
  section specific to that plugin.
- Resourcedef definition are not required.
- RUR variables are written out as PBS custom resources and appear in PBS accounting logs but not qstat -xf
- Each plugin is different enough to require different treatment.
- Timestamp plugin is a simple single string.
- Taskstats and Energy plugins are assumed to be numerical values and have a single line in RUR output per job - ie,
  they do not need to be added (+=) together
- Memory plugins can have multiple lines in RUR file - one line contributed by each compute node. 
- Memory plugins contain some metrics which are numerical and others are strings. To be maintain code efficiency, avoid 
  checks using if loops. Instead a two separate lists of metrics are used, one for numerical metrics (rur_memory_keys_toadd), 
  the other for string metrics (rur_memory_keys_tojoin).


Caveats:
- Not all PEP8 rules may be followed.
- Some metrics contain ':' in the name itself which is an illegal resourcedef entry, therefore it will be skipped,
- Future/Newer plugin metrics needs to be handled specifically based on the general template in this program.
- Currently ALL metric value(s) are treated as either string or long by PBS in the resourcedef file. Based on
  requirement it can be modified to an array for PBS , however the handling for those specific metrics needs to be
  changed else PBS will reject them.

Implementation:
- Modify resourcedef entries with the metrics that need to be accounted ( Refer sample resourcedef file )
- Define pbs-rur-accounting hook within PBS Professional
- qmgr -c "create hook pbs-rur-accounting"
- qmgr -c "set hook pbs-rur-accounting event = execjob_epilogue"
- qmgr -c "set hook pbs-rur-accounting enabled = true"
- qmgr -c "import hook pbs-rur-accounting application/x-python default <path_to_pbs-rur-accounting.py>"

History:
    29Jun2016   CC  Add sleep.time(), otherwise there will be an error like: Error in opening file
                    /var/spool/PBS/spool/rur.<jobid>
                    The reason is this hook will activate before the rur.<jobid> file has been written completely.
                    Workaround is to check for file existence before opening. Loop for MAX_CHECK_FILE times, and wait 
                    for WAIT_CHECK_FILE, if still not found then reject hook.
    29Jun2016   CC  Change Regex from ".... plugin: (.*?) \{(.*)\}" to ".... plugin: (.*?) [\{\[](.*)[\]\}]"
                    because some metrics have curly and some have square brackets
    29Jun2016   CC  Error in for loop indexing, eg for i in range(0, len(list_meminfo), 2).
                    For an empty string array, length is still counted as 1 instead of 0. Added IF condition preceding it.
    31Oct2016   CC  Fixed: Taskstats metric used a different syntax than previously thought. It now uses ['blah']
    28Apr2017   CC  Modified pbs.conf area so that it catches empty lines and comments

'''

import os
import re
import sys
import exceptions
import traceback
import time

##### CONFIGURATION SETTINGS ######
# This section contains CONSTANTS that can be modified to configure this for the system
import pbs


"""
This function checks if the exact same word is present in a line or not. Used to match resourcedef entries with
metric names provided by the RUR.
"""
def isPresent(word,line):
    present = re.search('\\b'+word+'\\b',line)
    return present


"""
This functions populates the RUR's timestamp plugin's metric(s). This classifies the metric values as a string.
Please ensure that the type of the metric's value matches with type of the value as defined in the resourcedef file.
(Refer Caveats section for future changes)
"""
def populate_rur_timestamp_metric(metric_data):
    if DEBUGON:
        print "in populate_rur_timestamp_metric()"
        print str(metric_data)
    if PBS_ENVIRONMENT:
        #pbs.logmsg(pbs.LOG_DEBUG, sprefix + "plugin detailsX : " + str(metric_data))
        e.job.resources_used["timestamp"] = str(metric_data)


sprefix = "CCpbs_rur_accounting: "   # added by CC

# Set to 1 for debug messages. IMPORTANT: This option is NOT compatible with PBS Professional hooks
# IOW, do not enable if you intend to import the python code into a PBS hook.
# 0 -> on PBS server,  1 -> on test environment
DEBUGON = 0

# Set to 1 when hook is being executed by PBS Professional; 
# 0 -> on test environment, 1,2,etc -> on PBS server
PBS_ENVIRONMENT = 1

if PBS_ENVIRONMENT:
    e = pbs.event()
    PBS_JOBID = e.job.id       # LIVE - Production
    #PBS_JOBID = '1'             # Testing
else:
    PBS_JOBID = '1'    # expects file "rur.0"
#    PBS_JOBID = '944.opal-p1'

# Max number of tries to check file before exit
MAX_CHECK_FILE=5
# Seconds to wait, before checking file existence again
WAIT_CHECK_FILE=1

# DO NOT MODIFY CODE BEYOND THIS SECTION
##### END CONFIGURATION SETTINGS ######


"""
Source PBS_CONF_FILE into environment AND construct common PBS commands
Source in the /etc/pbs.conf; it is 'good practice' to incorporate this so that
you are referring to the correct PBS Professional commands. Reduces some
hard coding, too.
"""
os.environ['PBS_CONF_FILE'] = '/etc/pbs.conf'
if os.path.isfile(os.environ['PBS_CONF_FILE']):
    pbs_conf = open(os.environ['PBS_CONF_FILE'], 'r')
    #for line in pbs_conf:
    #    os.environ[line.split("=")[0]] = line.split("=")[1].strip('\n')
    #pbs_conf.close()
    #with open(os.environ['PBS_CONF_FILE'], 'r') as f_in:
    for line in pbs_conf:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if '=' in line:
            os.environ[line.split("=")[0]] = line.split("=")[1].strip('\n')
    #print "helloW"   
else:
    if PBS_ENVIRONMENT:
        pbs.logmsg(pbs.LOG_DEBUG,"Unable to find PBS_CONF_FILE ... " + os.environ['PBS_CONF_FILE'])
    else:
        print "Unable to find PBS_CONF_FILE ... " + os.environ['PBS_CONF_FILE']
    sys.exit(1)

"""
Locate the job's RUR output file
The execjob_epilogue hook is expecting the RUR output to be in $PBS_HOME/spool with the
file name conforming to rur.$PBS_JOBID.
If the file does not exist, then the hook will log the error and bail.
"""
try:
    filename = os.environ['PBS_HOME'] + "/spool/rur."  + PBS_JOBID
    #filename = os.environ['PBS_HOME'] + "/spool/rur.500"
    #pbs.logmsg(pbs.LOG_WARNING, "checking rur.500 ... " + filename)
except SystemExit:
    pbs.logmsg(pbs.LOG_WARNING, "potential file not found problem  ... " + filename)
    pass
except:
    e.reject("%s hook failed with %s. Please contact Admin" % (e.hook_name, sys.exc_info()[:2]))

"""
The rur_data dictionary(s). The *_final dictionaries will contain nested dictionaries for the
different Cray plugins we will be monitoring (e.g., energy, taskstats, memory, timestamp) based on their presence in
the resourcedef file. We need to store the default supported metric names in order to make things easier to rename ,thus
avoiding ambiguity.
"""
rur_data = {}
resourcedef_path = os.environ['PBS_HOME'] + "/server_priv/resourcedef"
if DEBUGON:
    print resourcedef_path 


rur_taskstats_keys = ["bkiowait", "etime", "minfault", "wcalls", "stime", "btime", "pgswapcnt", "abortinfo", "max_vm", "rcalls", "max_rss", "coremem", "majfault", "wchar", "utime", "exitcode", "rchar", "comm", "core", "ecode",\
"vm", "gid", "jid", "nice", "nid", "pid", "pjid", "ppid", "prid", "rss", "sched", "uid","exitcode:signal"]

rur_energy_keys = ["energy_used", "nodes", "nodes_cpu_throttled", "nodes_memory_throttled", "nodes_power_capped", "nodes_throttled",\
"nodes_with_changed_power_cap", "max_power_cap", "max_power_cap_count", "min_power_cap", "min_power_cap_count", \
"accel_energy_used", "nodes_accel_power_capped", "max_accel_power_cap", "max_accel_power_cap_count", "min_accel_power_cap", "min_accel_power_cap_count",\
"traceback", "type", "value", "nid", "cname"]

rur_memory_keys = ["percent_bootmem","Active(file)","boot_freemem","current_freemem","free","Inactive(file)",\
"nr","resv","surplus","Active_anon", "Slab", "Inactive_anon", \
"traceback", "type", "value", "nid", "cname"]
rur_memory_keys_toadd = ["Active_file","boot_freemem","current_freemem","free","Inactive_file",\
"nr","resv","surplus","Active_anon", "Slab", "Inactive_anon"]
rur_memory_keys_tojoin = ["percent_bootmem","traceback", "type", "value", "nid", "cname"]

# DEBUG ONLY - REMOVE LATER
#if PBS_ENVIRONMENT:
#    for aaa in ["boot_freemem", "current_freemem", "nr", "surplus","Active_anon", "Slab", "Inactive_anon"] :
#    for aaa in ["Active_file"] :
#        e.job.resources_used['mem_'+aaa] = " "
#        pbs.logmsg(pbs.LOG_DEBUG, sprefix + 'mem_'+aaa)

plugin_list = ["memory", "taskstats", "energy"]
plugin_pref = ["mem_", "tst_", "eng_"]



#Populate and report the metrics to PBS
#populate_rur_json_data(filename)   # CC removed function definition and make it into the main body, also define pbs_rur_file
pbs_rur_file = filename

"""
This functions performs the actual text processing of the json based output of the RUR. Since the python json module is
not supported we convert the json formatted data into a dict for further classification and processing. Standard pattern
matching regexes are used to achieve this. Certain metrics are ignored (Refer Assumption section) and also some special
metrics are reported here itself and removed from the dict in order to generate dict based data without with a standard
key,value pair(s) based format i.e No nested dict(s) and/or arrays are processed further.
(Refer Caveats section for future changes)
"""

#def populate_rur_json_data(pbs_rur_file):
try:
    is_RUR_FILE_OPEN=False     # to be used only in finally clause, before closing file.
    if DEBUGON:
        print "RUR file: " + pbs_rur_file
    for ii in range(1, MAX_CHECK_FILE+1):    # Python range (1,N) will iterate only to N-1
        if os.path.isfile(pbs_rur_file):
            rur_file = open(pbs_rur_file, 'r')
            is_RUR_FILE_OPEN=True
            break
        else:
            if ii == MAX_CHECK_FILE:
                if PBS_ENVIRONMENT:
                    pbs.event().reject("File not found - %s " % pbs_rur_file)
                    pbs.logmsg(pbs.LOG_DEBUG,"Unable to find RUR.JOBID - " + pbs_rur_file)
                else:
                    print "Unable to find rur.JOBID file"
                sys.exit()
            else:
                time.sleep(WAIT_CHECK_FILE)

    #rur_file = open(pbs_rur_file, 'r')
    try:
        # counter multiple rur lines - only memory at this stage
        mem_lines = 0

        for line in rur_file:
            line = line.strip()
            if DEBUGON:
                print line
            
            # Sometimes plugins use [] or {} hence the [\{\[](.*)[\]\}]
            record_format_1 = re.search("uid: (.*?), apid: (.*?), jobid: (.*?), cmdname: .*?, plugin: (.*?) [\{\[](.*)[\]\}]", line)
            record_format_2 = re.search("uid: (.*?), apid: (.*?), jobid: (.*?), cmdname: .*?, plugin: timestamp (.*)", line)

            if record_format_1:
                jobid = record_format_1.group(3)
                plugin = record_format_1.group(4)
                metrics = record_format_1.group(5)
            elif (record_format_2):
                jobid = record_format_2.group(3)
                plugin = "timestamp"
                metrics = record_format_2.group(4)
            else:
                continue

            if DEBUGON:
                print "Metric " + metrics

            # If the plugin key does not exist, create the dictionary
            if plugin not in rur_data:
                if DEBUGON:
                    print "Does NOT Exist " + plugin + " Creating... \n"
                rur_data[plugin] = {}
            else:
                if DEBUGON:
                    print "Does Exist " + plugin + "\n"


# Explanation of keys, filters, PBS resources etc
# 1. plugin_metrics -> ONE raw line of data from RUR file. Double quotes removed
#     Any keys with weird characters, eg %_of_boot_mem, or subsets like meminfo, will be processed so that ->
# 2. plugin_metrics_dict -> will contain "proper" keys, ie no % characters, no subsets like meminfo
# 3. X_keys_final -> a list of keys that coincides with resourcedef. This set of keys represent the KNOWN keys.
#     For each of these X_keys_final, a check is made whether the dictionary has thme.
            #plugin_metrics = metrics.replace('"','')
            #plugin_metrics = metrics.replace('\'','')

            if plugin == 'taskstats':
                plugin_metrics = metrics.replace('"', '')
                plugin_metrics = metrics.replace('\'', '')
                plugin_metrics = plugin_metrics.replace(": ", ", ").split(", ")
                #pbs.logmsg(pbs.LOG_WARNING, '\n'.join(plugin_metrics[:])  )
                plugin_metrics_dict = dict(plugin_metrics[i:i+2] for i in range(0, len(plugin_metrics), 2))
                plugin_metrics_dict.pop("exitcode:signal",None)
                plugin_metrics_dict.pop("core",None)
                if DEBUGON:
                    print "plugin do : " + plugin
                if PBS_ENVIRONMENT:
                    pbs.logmsg(pbs.LOG_DEBUG, sprefix+"plugin done : " + plugin)
                #populate_rur_taskstats_metric(plugin_metrics_dict)
                # Similar to creating the rur_data[plugin], dynamically creating the metric dictionary
                # rur_data[plugin][metric] dictionary and provide value.
                # ASSUME: only one energy line, hence no need to +=
                #for metric in [key for key, value in plugin_metrics_dict.iteritems()]:
                #    pbs.logmsg(pbs.LOG_DEBUG, sprefix + "plugin metrix : " + metric)
                #for kk in rur_taskstats_keys:
                #    pbs.logmsg(pbs.LOG_DEBUG, sprefix + "plugin metrixAAA : " + kk)
                for metric in [key for key, value in plugin_metrics_dict.iteritems() if key in rur_taskstats_keys]:
                    rur_data[plugin][metric] = int(plugin_metrics_dict[metric])
                    #print metric + str(plugin_metrics_dict[metric])
                    #pbs.logmsg(pbs.LOG_DEBUG, sprefix + "plugin mett : " + metric)


            
            elif plugin == 'energy' :
                plugin_metrics = metrics.replace('"', '')
                plugin_metrics = plugin_metrics.replace(": ", ", ").split(", ")
                plugin_metrics_dict = dict(plugin_metrics[i:i+2] for i in range(0, len(plugin_metrics), 2))
                if DEBUGON:
                    print "plugin do : " + plugin
                if PBS_ENVIRONMENT:
                    pbs.logmsg(pbs.LOG_DEBUG, sprefix+"plugin done : " + plugin)
                #populate_rur_energy_metric(plugin_metrics_dict)

                # Similar to creating the rur_data[plugin], dynamically creating the metric dictionary
                # rur_data[plugin][metric] dictionary and provide value.
                # ASSUME: only one energy line, hence no need to +=
                for metric in [key for key, value in plugin_metrics_dict.iteritems() if key in rur_energy_keys]:
                    rur_data[plugin][metric] = int(plugin_metrics_dict[metric])

            
            elif plugin == 'timestamp' :
                if DEBUGON:
                    print "plugin do : " + plugin
                if PBS_ENVIRONMENT:
                    pbs.logmsg(pbs.LOG_DEBUG, sprefix+"plugin done : " + plugin)
                populate_rur_timestamp_metric(metrics)

            
            elif plugin == 'memory' :
                plugin_metrics = metrics.replace('"', '')
                mem_lines = mem_lines + 1
                if DEBUGON:
                    print "plugin do : " + plugin
                if PBS_ENVIRONMENT:
                    pbs.logmsg(pbs.LOG_DEBUG, sprefix+"plugin done : " + plugin)
                # MEMINFO
                # get substring   plugin_metrics_meminfo <- Active(anon): 35952, Slab: 105824, Inactive(anon): 1104
                plugin_metrics_meminfo=plugin_metrics.strip()[plugin_metrics.find('{A')+1:plugin_metrics.find('}')]
                plugin_metrics = plugin_metrics.replace('{'+plugin_metrics_meminfo+'}','NULL')
                # convert into list of key-values -> metrics_meminfo
                list_meminfo = plugin_metrics_meminfo.replace("(anon)","_anon").replace(": ", ", ").split(", ")

                # BOOTMEM
                # CC
                all_bootmem=plugin_metrics.strip()[plugin_metrics.find('%_of_boot'):plugin_metrics.find('],')]
                plugin_metrics = plugin_metrics.replace('' + all_bootmem + '], ', '')    # Removed from "%_of_boot..." to "....61.43],"  
                plugin_metrics_bootmem = all_bootmem.strip()[all_bootmem.find('[')+1:]   # should get: 67.23, 67.23, ...., 61.43
                

                #CC
                # Look for main key "hugep.." to last sub-key-val just before closing },
                #  hugepages-2048kB: {nr: 5120, surplus: 5120   # python automatically ignores double quotes
                all_hugepages=plugin_metrics.strip()[plugin_metrics.find('hug'):plugin_metrics.find('},')]
                plugin_metrics=plugin_metrics.replace('' + all_hugepages + '}, ', '')   # should become  meminfo: NULL, %_of_boot_mem:
                plugin_metrics_hugepages = all_hugepages.strip()[all_hugepages.find('{')+1:]   #should get:    nr: 5120, surplus: 5120
                
                list_hugepages = plugin_metrics_hugepages.replace(": ", ", ").split(", ")
                

                # Processed line from RUR file >>> Convert to dictionary
                plugin_metrics = plugin_metrics.replace(": ", ", ").split(", ")
                plugin_metrics_dict = dict(plugin_metrics[i:i+2] for i in range(0, len(plugin_metrics), 2))
                plugin_metrics_dict.pop("meminfo",None)
                plugin_metrics_dict.pop("%_of_boot_mem",None)

                # Special treatment for sub Key-Value structures
                #WARNING: Do not convert to integer yet, let them remain as strings
                plugin_metrics_dict['nid'] = "n" + str(plugin_metrics_dict['nid'])
                # Even EMPTY arrays [''] have length of 1. Since we are expecting key-val pairs, this checks for 2 elements or above.
                if (len(list_meminfo) > 1):
                    for i in range(0, len(list_meminfo), 2):
                        plugin_metrics_dict[str(list_meminfo[i])] = list_meminfo[i+1]
                if (len(list_hugepages) > 1):
                    for i in range(0, len(list_hugepages), 2):
                        plugin_metrics_dict[str(list_hugepages[i])] = list_hugepages[i+1]
                plugin_metrics_dict['percent_bootmem'] = str(plugin_metrics_bootmem)

                #print plugin_metrics_dict
                #populate_rur_memory_metric(plugin_metrics_dict, mem_lines)

                # Similar to creating the rur_data[plugin], dynamically creating the metric dictionary
                # rur_data[plugin][metric] dictionary and provide value.
                for memory_metric in [key for key, value in plugin_metrics_dict.iteritems() if key in rur_memory_keys_toadd]:
                    if memory_metric not in rur_data[plugin]:
                        rur_data[plugin][memory_metric] = int(plugin_metrics_dict[memory_metric])
                    else:
                        rur_data[plugin][memory_metric] += int(plugin_metrics_dict[memory_metric])

                for memory_metric in [key for key, value in plugin_metrics_dict.iteritems() if key in rur_memory_keys_tojoin]:
                    if memory_metric not in rur_data[plugin]:
                        rur_data[plugin][memory_metric] = str(plugin_metrics_dict[memory_metric])
                    else:
                        rur_data[plugin][memory_metric] += ", " + str(plugin_metrics_dict[memory_metric])

            else:
                pbs.logmsg(pbs.LOG_DEBUG,"Unknown plugin")

            if DEBUGON:
                print "plugin done : " 
            if PBS_ENVIRONMENT:
                pbs.logmsg(pbs.LOG_DEBUG, sprefix+"plugin done " )

        # End of looping over lines of RUR file

        # Filling in PBS custom resources (e.job...) from data structure (rur_data)
        # All custom resource will be strings

        if PBS_ENVIRONMENT:
            for ii in range(len(plugin_list)):
                prfx = plugin_pref[ii]
                for metric in rur_data[plugin_list[ii]]:
                    e.job.resources_used[prfx +metric] = str(rur_data[plugin_list[ii]][metric])


#    except ValueError:
#        pbs.logmsg(pbs.LOG_DEBUG,sprefix+" ValueError in populate_rur_json_data")        
#    except:
#        pbs.logmsg(pbs.LOG_DEBUG,sprefix+" Error in populate_rur_json_data")
#        e.reject("Gen 2: %s hook failed with %s" % (e.hook_name, sys.exc_info()[:2]))
    finally:
        if is_RUR_FILE_OPEN:
            rur_file.close()
                
except SystemExit:
    if DEBUGON:
        print "System Exit"
    if PBS_ENVIRONMENT:
        pbs.logmsg(pbs.LOG_DEBUG, sprefix + "in Exc-SystemExit" )
        pbs.logmsg(pbs.LOG_DEBUG, "SysExit: %s hook failed with %s" % (e.hook_name, sys.exc_info()[:2]))
    pass
except IOError:
    if DEBUGON:
        print sprefix+" Error in opening file - "+pbs_rur_file
    if PBS_ENVIRONMENT:
        pbs.logmsg(pbs.LOG_DEBUG, sprefix+" Error in opening file - "+pbs_rur_file)
except:
    if DEBUGON:
        print sprefix+" General Error - "
        print str(traceback.format_exc().strip().splitlines())
        #msg = ("%s Unexpected error in %s handling %s event" %(sprefix, e.hook_name, hooks.event_name(e.type)))
        msg = (": %s %s" % (exc.__class__.__name__, str(exc.args)))
        print msg
    if PBS_ENVIRONMENT:
        #pbs.logmsg(pbs.LOG_DEBUG, sprefix+" General Error")
        #e.reject("Gen1: %s hook failed with %s" % (e.hook_name, sys.exc_info()[:2]))
        # Alexis
        pbs.logmsg(pbs.EVENT_DEBUG, str(traceback.format_exc().strip().splitlines()))
        msg = ("%s Unexpected error in %s handling %s event" %(sprefix, e.hook_name, hooks.event_name(e.type)))
        msg += (": %s %s" % (exc.__class__.__name__, str(exc.args)))
        pbs.logmsg(pbs.EVENT_ERROR, msg)
        e.reject(msg)



# Finally we clean up the rur_file
if DEBUGON:
    # DO NOT REMOVE file when testing
    print sprefix+" Completed"
if PBS_ENVIRONMENT:
    #os.remove(filename)
    pbs.logmsg(pbs.LOG_DEBUG, sprefix+" Completed")


# TO DO
# os.remove(filename) - UNcomment again
