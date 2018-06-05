#args<-commandArgs(T)
#read_path = args[1]
#R CMD BATCH D:\IdeaWorkSpace\HbaseQuery\convert.r D:\\IdeaWorkSpace\\HbaseQuery\\
# Rscript D:\IdeaWorkSpace\HbaseQuery\convert.r
#convert
library(data.table) # for fread
read_path='D:\\IdeaWorkSpace\\HbaseQuery\\'
data     = fread(paste0(read_path,'result.csv') )
library(reshape2)
result<-dcast(data,glass_id~param_name)
write.csv(result, file = paste0(read_path,'data.csv'),row.names = FALSE)


#data cleaning
library(bit64)
library(bit)
# input: 
rawdata     = fread(paste0(read_path,'data.csv') )
col_name = data.frame(attributes(rawdata)$names)
variable_category = c('label','_id','_name','_vendor','_eqp_id','_time')
pth      = 0.03

# function:
# Variable_Type_fun  : 標記常數離散連續和一些敘述統計量
# Delete_outlier_fun : 將 outlier 補成 NA
# Rand_Index_fun     : 計算 Rand Index matrix
# Pearson_Cor_fun    : 計算 Pearson Correlation Coefficient matrix
# Delete_highCor_fun : 將 高相關變數去除 
Variable_Type_fun  <- function(data,col_name,variable_category,pth){
  

  # col_name = fread(paste0(read_path,'t_loff_colnames.csv'))
  # variable_category = c('label','_id','_name','_vendor','_eqp_id','_time')
  # pth = 0.03
  # data , col_name are dataframe
  
  Variable_fun <- function(X){
    # X = data[,120]
    # X = data[,31849]
    DorCfun <- function(x){

      
      T1 = as.matrix(x[which(!is.na(x))])
      nrow = dim(T1)[1]
      ncol = dim(T1)[2]
      label_c = 0
      
      #1. 用 sd 判定是否為 "常數"
      if(nrow>1 && sd(T1)<10^(-7)){ label_c = 1 }
      if(length(unique(T1))==1){ label_c = 1 }
      
      #2. 
      if(label_c==0){
        
        ul = ll= ulv = llv = 0
        if(length(unique(T1))<6){ # 如果 unique個數小於6個則直接計離散
          
          label_c = 2
          
        }else{
          
          Q = quantile(T1, probs = c(0.25,0.5,0.75), na.rm = T)
          ulv = Q[3]+2*(Q[3]-Q[1])
          llv = Q[1]-2*(Q[3]-Q[1])
          if(Q[2]!=0){a3 = (ulv-llv)/abs(Q[2])}else{a3 = ulv-llv}
          if(a3<0.0001){label_c = 1}else{ ul = length(which(T1>ulv)); ll = length(which(T1<llv))}
          
        }
      }
      
      #3.
      if(label_c==0){
        
        oc1 = rep(0,nrow)
        oc2 = matrix(0,nrow,ncol)
        pos1 = which(T1>ulv)
        if(is.null(pos1)==F && length(pos1)/nrow<0.01){oc1[pos1]=1}
        pos2 = which(T1<llv)
        if(is.null(pos2)==F && length(pos2)/nrow<0.01){oc1[pos2]=1}
        #以上判定是否為outlier,是outlier則計1
        
        if(sum(oc1)!=0){
          T2 = T1[oc1==0]#把不是outlier的值抓出來
          if(length(unique(T2))<6){ # 如果 unique個數小於6個則直接計離散
            
            label_c = 2
            
          }else{
            
            Q2 = quantile(T2, probs = c(0.25,0.5,0.75), na.rm = T)
            a1 = Q2[3]+2*(Q2[3]-Q2[1])
            a2 = Q2[1]-2*(Q2[3]-Q2[1])
            if( a1-a2 <0.00001){ label_c1 = 1 }
            
          }
          
        }
        
      }
      
      # 如果前面都沒有被分到，則定義為連續
      if(label_c == 0){output = 3}else{output = label_c}
      
      return(output)
    }
    M = rep(0,9)
    nrow = dim(X)[1]
    nth = round(nrow*0.03)
    if( is.character(as.matrix(X[1])) ){ #如果是文字則計文字
      
      sub = as.matrix(X)
      sub2 = sub[which(sub!='')]
      result = tryCatch({as.numeric(sub2)}, warning = function(w) {}, error = function(e) {er = 1}, finally = {})
      if(!is.null(result)){ #就是文字
        N = length(sub2)
        if(N==0){M = rep(-1,9)}
        if(N<=nth){M = rep(-2,9)}
        if(M[1]==0){
          M[1] = 4
          M[2:9] = rep(-3,8)
          M[3] = length(unique(sub2))
        }
      }else{
        text_num = length(which(is.na(as.numeric(sub2))))
        numb_num = length(sub2)-text_num
        if(text_num>=numb_num){ #就是文字
          if(text_num==0){M = rep(-1,9)}
          if(text_num<=nth){M = rep(-2,9)}
          if(M[1]==0){
            M[1] = 4
            M[2:9] = rep(-3,8)
            M[3] = length(unique(sub2[is.na(as.numeric(sub2))]))
          }
        }else{ #就是數字
          sub3 = as.numeric(sub2[which(!is.na(as.numeric(sub2)))])
          N = length(sub3)
          if(N==0){M = rep(-1,9)}
          if(N<=nth ){M = rep(-2,9)}
          if(M[1]==0){
            M[1] = DorCfun(sub3)
            M[2:9] = c(sd(sub3),length(unique(sub3)),as.numeric(min(sub3)),as.numeric(quantile(sub3,c(0.25,0.5,0.75))),as.numeric(mean(sub3)),as.numeric(max(sub3))) 
          }
        }
      }
      
    }else{ #非文字的變數進入計算
      
      pos = which(!is.na(X)==T)
      N = length(pos)
      if(N==0){M = rep(-1,9)}
      if(N<=nth ){M = rep(-2,9)}
      if(M[1]==0){
        
        sub = X[[1]][pos]
        M[1] = DorCfun(sub)
        M[2:9] = c(sd(sub),length(unique(sub)),as.numeric(min(sub)),as.numeric(quantile(sub,c(0.25,0.5,0.75))),as.numeric(mean(sub)),as.numeric(max(sub))) 
        
      }
      
    }
    return(M)
  }
  
  
  nrow = dim(data)[1]
  ncol = dim(data)[2]
  nth = round(nrow*pth)
  
  M = matrix(0, nrow = ncol, ncol = 10)
  #1. 貼上 variable_category 的標籤
  for( i in 1:length(variable_category)){
    if((i-1)==0){
      M[grep(paste0('\\',variable_category[i]),as.matrix(col_name)),1] = i-2
    }else{
      M[grep(paste0('\\',variable_category[i]),as.matrix(col_name)),1] = i-1
    }
  }
  
  #2. 貼上 Variable_Type 的標籤
  for(i in 1:ncol){
    X = data[,i,with = F]
    M[i,2:10] = Variable_fun(X)
  }
  
  M = cbind(col_name,M)
  colnames(M) <- c("Variable_Name","Is_ID_or_Name_Y","Type","Std","Unique","Min","Q1","Q2","Q3","Mean","Max")
  
  data1 = data[,intersect(which(M$Is_ID_or_Name_Y==0),which(M$Type==2)),with=FALSE]
  data2 = data[,intersect(which(M$Is_ID_or_Name_Y==0),which(M$Type==3)),with=FALSE]
  z1 = matrix(0,dim(data1)[2],1)
  for(i in 1:dim(data1)[2]){if(is.character(data1[[i]][1])==T){z1[i] = 1}}
  if(sum(z1)!=0){
    pos = which(z1==1)
    for(j in 1:length(pos)){data1[[pos[j]]] = as.numeric(data1[[pos[j]]])}
  }
  z2 = matrix(0,dim(data2)[2],1)
  for(i in 1:dim(data2)[2]){if(is.character(data2[[i]][1])==T){z2[i] = 1}}
  if(sum(z2)!=0){
    pos = which(z2==1)
    for(j in 1:length(pos)){data2[[pos[j]]] = as.numeric(data2[[pos[j]]])}
  }
  
  output = list()
  output$Data_Discrete = data1
  output$Data_Continuous = data2
  output$Variable_Type = M
  
  return(output)
}
Delete_outlier_fun <- function(M,data,num_type){
  
  # M = fread(paste0(save_path,'Variable_Type.csv') )
  # data = fread( paste0(save_path,'Data_Discrete.csv') ) 
  # M , data are dataframe; num_type = 2or3
  
  data = as.matrix(data)
  M1_V = M[intersect(which(M$Is_ID_or_Name_Y==0),which(M$Type==num_type)),]
  nrow = dim(data)[1]
  ncol = dim(data)[2]
  
  for(i in 1:ncol){
    pos = which(!is.na(data[,i])==T)
    sub = data[pos,i]
    th_l = M1_V$Q1[i]-1.5*(M1_V$Q3[i]-M1_V$Q1[i])
    th_u = M1_V$Q3[i]+1.5*(M1_V$Q3[i]-M1_V$Q1[i])
    p1 = pos[c(which(sub<th_l),which(sub>th_u))]
    if(length(p1)!=0){data[p1,i] = rep(NaN,length(p1))}
  }
  return(data)
}
Rand_Index_fun     <- function(data){
  
  #data = fread( paste0(save_path,'Data_Discrete_without_outlier.csv') ) 
  library(EMCluster, quietly = TRUE)
  data = as.matrix(data)
  
  nrow = dim(data)[1]
  ncol = dim(data)[2]
  R = matrix(0, nrow = ncol, ncol = ncol)
  for(j in 1:(ncol-1)){
    p1 = which(!is.na(data[,j])==T)
    for(i in (j+1):ncol){
      p2 =  which(!is.na(data[,i])==T)
      pp = intersect(p1,p2)
      if(length(pp)>1){
        a1 = data[pp,j]
        a2 = data[pp,i]
        v1 = v2 = rep(0,length(pp))
        un1 = unique(a1)
        un2 = unique(a2)
        for(k1 in 1:length(un1)){v1[which(a1==un1[k1])] = k1}
        for(k1 in 1:length(un2)){v2[which(a2==un2[k1])] = k1}
        R[j,i] = RRand(v1, v2)$Rand
      }
    }
  }
  
  return(R)
}  
Pearson_Cor_fun    <- function(data){
  
  # library(EMCluster, quietly = TRUE)
  # data = fread( paste0(save_path,'Data_Continuous_without_outlier.csv') )
  
  R = cor(data, use="pairwise.complete.obs")
  R[lower.tri(R)]=0
  diag(R)= rep(0,dim(R)[2])
  return(R)
}
Delete_highCor_fun <- function(R,data,num_type){
  # R    = fread( paste0(save_path,'R_Discrete.csv') )
  # data = fread( paste0(save_path,'Data_Discrete.csv') ) 
  
  library(pracma)
  data = as.matrix(data)
  if(num_type==3){R = abs(R)}
  
  P1 = rep(0,dim(R)[1])
  th = 0.9;
  v1 = v2 = c() # v2就是要留下來的變數
  f <- function(x) sum(which(!is.nan(x)==T))     # function not vectorized
  for(i in 1:dim(R)[1]){
    if(i==1){
      x = R[i,]
      pos = c(i,which(x>th))
      R[pos,] = 0
      R[,pos] = 0
      v1 = unique(c(v1,pos))
      if(length(pos)==1){
        v2 = c(v2,pos)
        
      }else{
        zz = colSums(arrayfun(f, data[,pos]))
        v2 = c(v2,pos[which.max(zz)])
      }
      
    }else{
      if(length(which(v1==i))!=1){
        x = R[i,]
        pos = c(i,which(x>th))
        R[pos,] = 0
        R[,pos] = 0
        v1 = unique(c(v1,pos))
        if(length(pos)==1){
          v2 = c(v2,pos)
          
        }else{
          zz = colSums(arrayfun(f, data[,pos]))
          v2 = c(v2,pos[which.max(zz)])
        }
      }
    }
  }
  
  output = list()
  output$data = data[,v2]
  output$pos = v2
  return(output)
}  


t1 = proc.time()
output = Variable_Type_fun(rawdata,col_name,variable_category,pth)
TV = proc.time()-t1
write.csv(output$Variable_Type, file = paste0(read_path,'Variable_Type.csv'),row.names=FALSE)

t2 = proc.time()
output11 = Delete_outlier_fun(output$Variable_Type,output$Data_Discrete,2)
output12 = Rand_Index_fun(output11)
output13 = Delete_highCor_fun(output12,output$Data_Discrete,2)
TD = proc.time()-t2;TD

t3 = proc.time()
output21 = Delete_outlier_fun(output$Variable_Type,output$Data_Continuous,3)
output22 = Pearson_Cor_fun(output21)
output23 = Delete_highCor_fun(output22,output$Data_Continuous,3)
TC = proc.time()-t3;TC


#2 Data_Discrete_without_outlier.csv
pos = intersect(which(output$Variable_Type$Is_ID_or_Name_Y==0),which(output$Variable_Type$Type==2))
colnames(output11) = t(output$Variable_Type[pos,1])
write.csv(output11, file = paste0(read_path,'Data_Discrete_without_outlier.csv'),row.names=FALSE)

#3 Data_Discrete_without_outlier_highcor.csv
pos = intersect(which(output$Variable_Type$Is_ID_or_Name_Y==0),which(output$Variable_Type$Type==2))
colnames(output13$data) = t(output$Variable_Type[pos[output13$pos],1])
write.csv(output13$data, file = paste0(read_path,'Data_Discrete_without_outlier_highcor.csv'),row.names=FALSE)

#4 Data_Continuous_without_outlier.csv
pos = intersect(which(output$Variable_Type$Is_ID_or_Name_Y==0),which(output$Variable_Type$Type==3))
colnames(output21) = t(output$Variable_Type[pos,1])
write.csv(output21, file = paste0(read_path,'Data_Continuous_without_outlier.csv'),row.names=FALSE)

#5 Data_Continuous_without_outlier_highcor.csv
pos = intersect(which(output$Variable_Type$Is_ID_or_Name_Y==0),which(output$Variable_Type$Type==3))
colnames(output23$data) = t(output$Variable_Type[pos[output23$pos],1])
write.csv(output23$data, file = paste0(read_path,'Data_Continuous_without_outlier_highcor.csv'),row.names=FALSE)

#6 Data_Mix.csv
output3 = data[,which(output$Variable_Type$Is_ID_or_Name_Y!=0),with = F]
colnames(output3)<-t(output$Variable_Type[which(output$Variable_Type$Is_ID_or_Name_Y!=0),1])
write.csv(output3, file = paste0(read_path,'Data_Mix.csv'),row.names=FALSE)

#7.Y_label.csv
output4 = data[,which(output$Variable_Type$Is_ID_or_Name_Y==-1),with = F]
colnames(output4)<-t(output$Variable_Type[which(output$Variable_Type$Is_ID_or_Name_Y==-1),1])
write.csv(output4, file = paste0(read_path,'Y_label.csv'),row.names=FALSE)

