//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_MODIFICATION_H
#define TVG_IGOR_MODIFICATION_H

#include <vector>
#include <ctime>


#include "DBSecurityScheme.h"
#include "../../../../include/Globals.h"
#include "../../General/Header/tvg_index.h"
#include "../../General/Header/tvg_vertex_id.h"
#include "../../General/Header/tvg_time.h"



enum class MODIFICATION_TYPE {ADDITION, DELETION, REPLACEMENT};


class Modification
{
protected:
    //


    tvg_time m_tEpoch;
    MODIFICATION_TYPE m_typeModification;

    tvg_vertex_id m_lSource;
    tvg_vertex_id m_lDestination;

    const tvg_long_counter m_uid;

    const bool m_bReversed;



public:
    //
    Modification(tvg_long_counter uid, bool bReversed = false, MODIFICATION_TYPE m = MODIFICATION_TYPE::ADDITION): m_typeModification(m), m_uid(uid), m_bReversed(bReversed)
    {

    }

    Modification(const Modification& m) : m_tEpoch(m.m_tEpoch), m_typeModification(m.m_typeModification), m_lSource(m.m_lSource), m_lDestination(m.m_lDestination), m_uid(m.m_uid), m_bReversed(m.m_bReversed)
    {

    }




    Modification(const tvg_long_counter& lUID, const tvg_time& tEpoch, const MODIFICATION_TYPE& typeModification,
                 const tvg_vertex_id& lSource,  const tvg_vertex_id& lDestination, bool bReversed)
            : m_uid(lUID), m_tEpoch(tEpoch), m_typeModification(typeModification), m_lSource(lSource), m_lDestination(lDestination),  m_bReversed(bReversed)
    {

    }

    ~Modification()
    {

    }

    /*
    std::unique_ptr<Modification> ReverseModification()
    {
        std::unique_ptr<Modification> pReversedModification = std::make_unique<Modification>(m_uid, !m_bReversed);
        pReversedModification->m_lID = m_lID;
        pReversedModification->m_tEpoch = m_tEpoch;
        pReversedModification->m_typeModification = m_typeModification;
        pReversedModification->m_lSource = m_lDestination;
        pReversedModification->m_lDestination = m_lSource;

        return std::move(pReversedModification);
    }
*/


    bool GetIsReversed() const
    {
        return m_bReversed;
    }

    tvg_vertex_id GetSource() const
    {
        return m_lSource;
    }

    tvg_vertex_id GetDestination() const
    {
        return m_lDestination;
    }


    tvg_time GetEpoch() const
    {
        return m_tEpoch;
    }

    MODIFICATION_TYPE GetModificationType() const
    {
        return m_typeModification;
    }


    void SetSource(const tvg_vertex_id & lSource)
    {
        m_lSource = lSource;
    }

    void SetDestination(const tvg_vertex_id & lDestination)
    {
        m_lDestination = lDestination;
    }

    void SetEpoc(const tvg_time & epoch)
    {
        m_tEpoch = epoch;
    }

    const tvg_long_counter GetUID() const
    {
        return m_uid;
    }
};



//
/////////////////////////////////////////////////////////////////////////////////
//


class DBVerticaSecurityInstance : public Modification
{
protected:
    //
    static DBSecurityScheme* m_pSchemeDBSecurity;




//	boolean m_bIsInfected;
//	boolean m_bIsDomainExists;
    double m_dInfectionConfidence;

 //   E_QUERY_TYPE m_eQueryType = E_QUERY_TYPE.NONE;


public:
    //

    DBVerticaSecurityInstance(tvg_long_counter uid/*ResultSet resultSet*/) : Modification(uid) // ???????????????? Load the queries
    {
    }


    static DBSecurityScheme* GetSecurityScheme()
    {
        return m_pSchemeDBSecurity;
    }


    static void SetDBScheme(DBSecurityScheme* scheme)
    {
        m_pSchemeDBSecurity = scheme;
    }




};



#endif //TVG_IGOR_MODIFICATION_H
