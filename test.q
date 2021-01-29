\d .t
n:ne:nf:ns:0
pt:{-2 $[first[x]~`..err;err;fail][x;y]}
i:{` sv"  ",/:` vs x}
ge:{if[not P;n+:1;ns+:1;:(::)];v:.Q.trp[x;y;{(`..err;x,"\n",.Q.sbt 1#y)}];n+:1;if[not(1b~v)|(::)~v;pt[v](y;file)]}
P:1;N:0;MM:0#`
e:ge value
err:{ne+:1;"ERROR:\n test:\n",i[y 0]," message:\n",i[x 1]," file:\n",i y 1}
fail:{nf+:1;"FAIL:\n test:\n",i[y 0]," result:\n",i[.Q.s x]," file:\n",i y 1}
u:raze{$[0>type k:key x;k;` sv'x,'k]}each hsym`$$[count .z.x;.z.x;enlist"tests"]
{N+:1;P::1;file::x;system"l ",x}each 1_'string u where u like"*.t"; 
msg:{", "sv{":"sv string(x;y)}'[key x;value x]}`failed`errored`skipped`total!nf,ne,ns,n;
if[(ne+nf);-2 msg;exit 1]
\\