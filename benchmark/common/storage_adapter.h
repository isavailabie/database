#pragma once
// Called only at quiescent checkpoints; never use the submitted stats as oracle.
namespace hw {
inline void DBState::Flush(){Ok(Impl()->TEST_CompactMemTable());env.Wait();}
inline void DBState::Compact(int last){Flush();for(int i=0;i<=last;++i) Impl()->TEST_CompactRange(i,nullptr,nullptr);env.Wait();}
inline void DBState::FullGC(){Compact();db->CompactRange(nullptr,nullptr);env.Wait();}
inline Physical ScanPhysical(DBState& s){
  s.env.Wait();Physical p;std::set<std::string> keys;
  std::unique_ptr<Iterator> it(s.Impl()->TEST_NewInternalIterator());
  for(it->SeekToFirst();it->Valid();it->Next()){
    Slice k=it->key();Check(k.size()>=8,"invalid internal key");keys.emplace(k.data(),k.size()-8);++p.total;
  }
  Ok(it->status());p.distinct=keys.size();return p;
}
}
