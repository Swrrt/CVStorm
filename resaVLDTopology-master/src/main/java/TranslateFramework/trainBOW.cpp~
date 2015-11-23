#include <cstdio>
#include <cstring>
#include <iostream>
#include <algorithm>
#include <vector>
#include <io.h>
#include <string>
#include <windows.h>
#include <ctime>
#include <cstdlib>
#pragma comment(lib,"User32.lib")
using namespace std;
const string path = "C:\\testing\\Features\\HOF0";
const string output = "C:\\testing\\trainedBOW_HOF0.txt";
const int itern = 100;
const int cn = 400;
int n,m;
double **data;
double **center,**vote,*s;
int main(){
	int iter;
	long hfile =0;
	struct _finddata_t file;
	string pathName,exdName;
	if((hfile=_findfirst(pathName.assign(path).append("\\*").c_str(), &file)) ==-1){
		return 0;
	}
	do{
		cout<<file.name<<endl;
		string name = path+("\\")+(file.name);
		FILE *fp = fopen(name.c_str(),"r");
		n=0;m=0;
		data = NULL;
		if(fscanf(fp,"%d",&n)){
			data = new double*[n];
		for(int i=0;i<n;i++){
			fscanf(fp,"%d",&m);
			data[i] = new double[m];
			for(int j=0;j<m;j++){
				fscanf(fp,"%lf",&data[i][j]);
			}
		}}
		fclose(fp);
		printf("%d %d\n",n,m);
	}while(_findnext(hfile, &file)==0);
	_findclose(hfile);
	printf("%d %d\n",n,m);
	srand((unsigned)time(NULL));
	center = new double*[cn];
	vote = new double*[cn];
	s = new double[cn];
	for(int i=0;i<cn;i++){
		center[i] = new double[m];
		vote[i] = new double[m];
		int k = rand()%n;
		for(int j=0;j<m;j++){
			center[i][j] = data[k][j];
		}
	}
	printf(" Prepare over\n");
	iter = itern;
	
	while(iter>0){
		printf(" Remaining iter : %d\n",iter);
		iter--;
		for(int i=0;i<cn;i++){
			s[i]=0;
			for(int j=0;j<m;j++)vote[i][j]=0;
		}
		double t1,t2;
		for(int i=0;i<n;i++){
			if(t2-t1>1e-8)printf(" Remaining time : %.5f\n", (iter*n+(n-i))*(t2-t1));
			t1 = time(NULL);
			double min = 1e10;
			int mini = 0;
			for(int j=0;j<cn;j++){
				double dis=0;
				for(int k=0;k<m;k++){
					dis += (data[i][k]-center[j][k])*(data[i][k]-center[j][k]);
				}
				if(dis<min){
					min = dis;
					mini =j;
				}
			}
			s[mini]++;
			for(int k=0;k<m;k++)vote[mini][k]+=data[i][k];
			t2 = time(NULL);
		}
		for(int i=0;i<cn;i++){
			if(s[i]>0){
				for(int k=0;k<m;k++){
					center[i][k] = vote[i][k]/s[i];
				}
			}
		}
	}
	FILE *fo = fopen(output.c_str(),"w");
	fprintf(fo,"%d %d\n",cn, m);
	for(int i=0;i<cn;i++){
		for(int j=0;j<m;j++){
			fprintf(fo,"%.15f ",center[i][j]);
		}
		fprintf(fo,"\n");
	}
	fclose(fo);
	return 0;
}

