import os
import sys

path = 'C:\\Users\\swrrt\\Downloads\\libsvm-3.20\\python'
sys.path.append(path)
os.chdir(path)
from svmutil import *
from decimal import Decimal
path = 'C:\\testing\\BOWFeatures'
fs = os.listdir(path)
oup = open(path+'\\Summation.txt','w')
result = []
for f in fs:
	fp = os.path.join(path,f)
	i = f.find('label_')
	if i != -1 :
		inp = open(fp,'r')
		for line in inp:
			result = list(line.split(' '))
		inp.close()
		oup.write(f[i+6])
		j=0
		for x in result:
			j = j+1
			if((x)!='0.0' and x!=''):
				oup.write('\t'+str(j)+':'+str(float(x)))
		oup.write('\n')
oup.flush()
oup.close()
y, x  = svm_read_problem(path+'\\Summation.txt')
m = svm_train(y[5:35],x[5:35], '-s 0 -b 1 -g 0.1 -e 0.001 -c 10 -m 1024')
p_label, p_acc,p_val = svm_predict(y[:5]+y[35:],x[:5]+x[35:],m)
print p_label,p_acc
svm_save_model('C:\\testing\\SVM.model',m)
