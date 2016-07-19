//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_DBSECURITYSCHEME_H
#define TVG_IGOR_DBSECURITYSCHEME_H


#include <cstdio>
#include <string>


enum E_DB_Source {DB_DMC, DB_S_FLOW, DB_TVG_01_APRIL};

class DBSecurityScheme
{
private:
    //
    const E_DB_Source m_eDBSource;


public:
    //
    DBSecurityScheme(E_DB_Source eDBSource) : m_eDBSource(eDBSource)
    {

    }

    E_DB_Source GetDB_Source()
    {
        return m_eDBSource;
    }


    virtual std::string GetTimestampString() = 0;
    virtual std::string GetSourceString() = 0;
    virtual std::string GetDestinationString() = 0;
    virtual std::string GetCatString() = 0;
    virtual std::string GetIsBlacklistedString() = 0;
    virtual std::string GetIsDomainExistsString() = 0;
    virtual std::string GetHashSourceString() = 0;
    virtual std::string GetHashDestinationString() = 0;
    virtual std::string GetHashAnswerIPDestinationString() = 0;
    virtual std::string GetInternalString() = 0;
    virtual bool IsInternal(std::string str) = 0;





    /*
    public abstract long GetTimestamp();
    public abstract std::string GetSource();
    public abstract std::string GetDestination();
    public abstract boolean IsBlacklisted();
    public abstract boolean IsDomainExists();
    public abstract long GetHashSource();
    public abstract long GetHashDestination();
    */
};

#endif //TVG_IGOR_DBSECURITYSCHEME_H
