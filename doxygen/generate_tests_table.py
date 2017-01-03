__author__ = 'roy'
import re

workingdir = '/Users/user/git/scylla-dtest/'

#data=open(workingdir+'scylla_tests').readlines()
#print data
with open(workingdir+'scylla_tests') as f:
    with open(workingdir+'doxygen/gen_tests_table.out', 'w') as f2:
        f2.write("| Functionality | Implemented Tests |\n")
        f2.write("| ------------- | :---------------- ")
        alist = f.read().splitlines()
        #print alist
        for line in alist:
            if line.startswith('**'):
                f2.write(" |" + "\n" + "| " + line + " | ")
            else:
                if ":" in line:
                    #print "BEFORE"+line
                    line=re.sub(".py",":",line)
                    #print "AFTER "+line
                    f2.write(line + "\\n ")
                else:
                    f2.write(line)

        f2.write(" | ")
