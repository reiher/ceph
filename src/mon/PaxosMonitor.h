class PaxosMonitor : public Paxos {

 private:
  Monitor *mon;

 protected:

  MonitorDBStore *get_store() { return mon->store; }
  int get_rank() { return mon->rank; }
  bool is_leader() { return mon->is_leader(); }
  bool is_peon() { return mon->is_peon(); }
  set<int>& get_quorum() { return mon->get_quorum(); }
  epoch_t get_epoch() { return mon->get_epoch(); }
  
  int send_message(Message *m, const entity_inst_t& dest) {
    return mon->messender->send_message(m, dest);
  }


 public:
  PaxosMonitor(Monitor *mon) { }

}
