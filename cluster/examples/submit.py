import cluster

for i in xrange(3):
  j = cluster.CondorJob('/bin/ls','-la')
  j.set_output('job%d.out'%i)
  j.add_requirement('InMastodon')
  pid = j.submit()
  print 'Job %d submitted with PID %d'%(i, pid)

cluster.list_jobs()
