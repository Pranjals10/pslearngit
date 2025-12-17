def normalize(x):
 out=[]
 for i in x:
  if i: out.append(i.strip().lower())
 return out
