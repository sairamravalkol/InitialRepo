//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_GLOBALS_H
#define TVG_IGOR_GLOBALS_H



typedef unsigned short tvg_short_counter;
typedef unsigned long tvg_counter;
typedef unsigned long long tvg_long_counter;


constexpr int ConstMax(int l,int r)
{
    return l<r ? r : l;
}

enum class SYS_STATE{BOTH,INSERT,SELECT};

//typedef unsigned long long tvg_time;

//class tvg_vertex_id;
//class tvg_index;





//typedef unsigned long long tvg_vertex_id;














#endif //TVG_IGOR_GLOBALS_H
