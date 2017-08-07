--result line
--test

with
    SQ_MANY_DIM_FCT_INAC_SRVC as(
        select /*+ use_hash(fct sr) leadind (fct) parallel(fct,4) parallel(sr,4)*/
            fct.DATE_KEY,
            fct.ban_key,
            fct.SUBS_KEY,
            fct.MARKET_KEY,
            fct.LAST_FTTB_SERVICE_KEY,
            fct.LAST_IPTV_SERVICE_KEY,
            fct.FTTB_SERVICE_CONNECT_IND,
            fct.IPTV_SERVICE_CONNECT_IND,
            fct.FTTB_SERVICE_CONNECT_IND_PRED,
            fct.IPTV_SERVICE_CONNECT_IND_PRED,
            fct.IPTV_BOX_IND,
            fct.MULTIROOM_IND,
            fct.IPTV_BOX_RENT_IND,
            fct.FTTB_TECH_SUPPORT_IND,
            fct.EFFECTIVE_DATE,
            fct.EXPIRATION_DATE,
            nvl(sr.BUNDLE_KEY,-99) BUNDLE_KEY
        from
            --fct
            (select /*+ parallel(a,4)*/
                to_date('17.07.2017','DD.MM.YYYY') as DATE_KEY,
                ban_key,
                SUBS_KEY,
                MARKET_KEY,
                --LAST_FTTB_SERVICE_KEY
                nvl(LAST_VALUE(FTTB_SERVICE_KEY IGNORE NULLS) over (partition by ban_key,
                                                                                SUBS_KEY,
                                                                                MARKET_KEY,
                                                                                BUSINESS_SERVICE_KEY
                                                                                order by RANK_IND DESC,
                                                                                        EFFECTIVE_DATE,
                                                                                        EXPIRATION_DATE,
                                                                                        SERVICE_KEY DESC
                                                                                ROWS BETWEEN
                                                                                    UNBOUNDED PRECEDING
                                                                                    AND UNBOUNDED FOLLOWING
                                                                    ),
                    '-99') as LAST_FTTB_SERVICE_KEY,
                --LAST_IPTV_SERVICE_KEY
                nvl(LAST_VALUE(IPTV_SERVICE_KEY IGNORE NULLS) over (partition by ban_key,
                                                                            SUBS_KEY,
                                                                            MARKET_KEY,
                                                                            BUSINESS_SERVICE_KEY
                                                                            order by RANK_IND DESC,
                                                                                    EFFECTIVE_DATE,
                                                                                    EXPIRATION_DATE,
                                                                                    SERVICE_KEY DESC
                                                                            ROWS BETWEEN
                                                                                UNBOUNDED PRECEDING
                                                                                AND UNBOUNDED FOLLOWING
                                                                    ),
                    '-99') as LAST_IPTV_SERVICE_KEY,
                FTTB_SERVICE_CONNECT_IND,
                IPTV_SERVICE_CONNECT_IND,
                FTTB_SERVICE_CONNECT_IND_PRED,
                IPTV_SERVICE_CONNECT_IND_PRED,
                IPTV_BOX_IND,
                MULTIROOM_IND,
                IPTV_BOX_RENT_IND,
                FTTB_TECH_SUPPORT_IND,
                EFFECTIVE_DATE,
                EXPIRATION_DATE
            from
                (select /*+ use_hash(fis disp) leading(disp) parallel(disp,4) parallel(fis,4)*/
                    fis.ban_key,
                    fis.SUBS_KEY,
                    fis.MARKET_KEY,
                    fis.DOMAIN,
                    fis.BUSINESS_SERVICE_KEY,
                    --FTTB_SERVICE_KEY
                    case
                        when fis.BUSINESS_SERVICE_KEY = 104009
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >= TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        fis.service_key
                    end as FTTB_SERVICE_KEY,
                    --IPTV_SERVICE_KEY
                    case
                        when fis.BUSINESS_SERVICE_KEY = 104023
                            and disp.param_name = 'IPTV_CAPSULE'
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >= TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        fis.service_key
                    end as IPTV_SERVICE_KEY,
                    --FTTB_SERVICE_CONNECT_IND
                    case
                        when fis.BUSINESS_SERVICE_KEY=104009
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as FTTB_SERVICE_CONNECT_IND,
                    --IPTV_SERVICE_CONNECT_IND
                    case
                        when fis.BUSINESS_SERVICE_KEY = 104023 and disp.param_name = 'IPTV_CAPSULE'
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >= TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as IPTV_SERVICE_CONNECT_IND,
                    --FTTB_SERVICE_CONNECT_IND_PRED
                    case
                        when fis.BUSINESS_SERVICE_KEY=104009
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY')
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY') - 1
                            then
                        1
                        else
                        0
                    end as FTTB_SERVICE_CONNECT_IND_PRED,
                    --asIPTV_SERVICE_CONNECT_IND_PRED
                    case
                        when fis.BUSINESS_SERVICE_KEY=104023 and disp.param_name = 'IPTV_CAPSULE'
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY')
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY') - 1
                            then
                        1
                        else
                        0
                    end as IPTV_SERVICE_CONNECT_IND_PRED,

                     -- новые индикаторы
                     --IPTV_BOX_IND
                    case
                        when fis.BUSINESS_SERVICE_KEY IN ('104028', '104038', '104039')
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as IPTV_BOX_IND,
                    --MULTIROOM_IND
                    case
                        when fis.BUSINESS_SERVICE_KEY IN ('104031')
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as MULTIROOM_IND,
                    --IPTV_BOX_RENT_IND
                    case
                        when fis.BUSINESS_SERVICE_KEY IN ('104028', '104039')
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as IPTV_BOX_RENT_IND,
                    --FTTB_TECH_SUPPORT_IND
                    case
                        when BUSINESS_SERVICE_KEY=104009 AND disp.param_name='LINE_HOLDER'
                            AND fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >=TO_DATE('17.07.2017','DD.MM.YYYY')
                            then
                        1
                        else
                        0
                    end as FTTB_TECH_SUPPORT_IND,
                    fis.EFFECTIVE_DATE,
                    fis.EXPIRATION_DATE,
                    fis.SERVICE_KEY,
                    --rank_ind
                    case
                        when fis.market_key= dic.market_key
                            then
                        0
                        when dic.market_key is not null
                            and fis.market_key <> dic.market_key
                            then
                        1
                        when dic.market_key is null
                            then
                        2
                        else
                        3
                    end as rank_ind
                from
                    etl2_etl.FCT_INAC_SERVICE@tstr15 fis,
                    --disp
                    (select /*+ parallel(a,4)*/
                        service_key,
                        param_name
                    from etl2_etl.DIM_INAC_SERVICE_PARAM@tstr15 a
                    where (param_name = 'IPTV_CAPSULE'
                            and active_ind = 1)
                        OR (param_name ='LINE_HOLDER'
                            and active_ind = 1)
                    ) disp,
                    etl2_etl.DIM_INAC_SERVICE@tstr15 dis,
                    etl2_etl.DIM_INAC_CITY@tstr15 dic
                 where fis.service_key = disp.service_key(+)
                     and fis.service_key = dis.service_key(+)
                     and dis.city_key= dic.city_key(+)
                     and fis.BUSINESS_SERVICE_KEY<> '-99'
                     and fis.BUSINESS_SERVICE_TYPE_KEY IN ('255', '259', '260', '262')
                     and (fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                            AND fis.EXPIRATION_DATE >= TO_DATE('17.07.2017','DD.MM.YYYY')
                        or fis.EFFECTIVE_DATE < TO_DATE('17.07.2017','DD.MM.YYYY')
                            AND fis.EXPIRATION_DATE >= TO_DATE('17.07.2017','DD.MM.YYYY') - 1
                        )
                ) a
            ) fct,
            --sr
            (select /*+ parallel(a,4)*/
                SERVICE_KEY,
                --BUNDLE_KEY
                MAX(case
                        when param_name ='IS_BUNDLE'
                            and active_ind = 1
                            then
                        ID
                        else
                        -99
                    end
                    ) as BUNDLE_KEY
            from etl2_etl.DIM_INAC_SERVICE_PARAM@tstr15 a
            group by SERVICE_KEY) sr
        where fct.LAST_FTTB_SERVICE_KEY = sr.SERVICE_KEY(+)
        order by fct.DATE_KEY,
                fct.ban_key,
                fct.SUBS_KEY,
                fct.MARKET_KEY
    ),
    AGGTRANS_FLTR_SRT as (
        select
            DATE_KEY,
            BAN_KEY,
            SUBS_KEY,
            MARKET_KEY,
            MAX(EFFECTIVE_DATE) as EFFECTIVE_DATE,
            MAX(EXPIRATION_DATE) as EXPIRATION_DATE,
            MAX(LAST_FTTB_SERVICE_KEY) as LAST_FTTB_SERVICE_KEY,
            MAX(LAST_IPTV_SERVICE_KEY) as LAST_IPTV_SERVICE_KEY,
            MAX(FTTB_SERVICE_CONNECT_IND) as FTTB_SERVICE_CONNECT_IND,
            MAX(IPTV_SERVICE_CONNECT_IND) as IPTV_SERVICE_CONNECT_IND,
            MAX(FTTB_SERVICE_CONNECT_IND_PRED) as FTTB_SERVICE_CONNECT_IND_PRED,
            MAX(IPTV_SERVICE_CONNECT_IND_PRED) as IPTV_SERVICE_CONNECT_IND_PRED,
            MAX(IPTV_BOX_IND) as IPTV_BOX_IND,
            MAX(MULTIROOM_IND) as MULTIROOM_IND,
            MAX(IPTV_BOX_RENT_IND) as IPTV_BOX_RENT_IND,
            MAX(FTTB_TECH_SUPPORT_IND) as FTTB_TECH_SUPPORT_IND,
            MAX(BUNDLE_KEY) as BUNDLE_KEY
        from SQ_MANY_DIM_FCT_INAC_SRVC
        group by DATE_KEY,
            BAN_KEY,
            SUBS_KEY,
            MARKET_KEY
        having MAX(EFFECTIVE_DATE) < trunc(DATE_KEY + 1,'DD')
            and MAX(EXPIRATION_DATE) >= DATE_KEY
        order by BAN_KEY,
            SUBS_KEY,
            MARKET_KEY
    ),
    SQ_DIM_INAC_SUBSCRIBER as (
        select /*+ use_hash(s c) leadind(c) parallel(c,4) parallel(s,4)*/
            s.BAN_KEY,
            s.SUBS_KEY,
            s.MARKET_KEY,
            s.DOMAIN,
            s.DW_STATUS,
            s.CITY_KEY,
            s.ADDRESS_KEY,
            s.JUR_IND,
            s.JUR_KEY,
            case
                when s.fttb_activation_date = to_date('17.07.2017','DD.MM.YYYY')
                    then
                1
                else
                0
            end as  FTTB_INFLOW_IND,
            case
                when s.iptv_activation_date = to_date('17.07.2017','DD.MM.YYYY')
                    then
                1
                else
                0
            end  as  IPTV_INFLOW_IND,
            s.fttb_activation_date,
            s.iptv_activation_date,
            c.FTTB_CHANEL_KEY as FTTB_CHANNEL_KEY,
            c.IPTV_CHANEL_KEY as IPTV_CHANNEL_KEY,
            to_date('17.07.2017','DD.MM.YYYY') as DATE_KEY
        from etl2_etl.DIM_INAC_SUBSCRIBER@tstr15 s,
            etl2_etl.DIM_INAC_CONTRACTS@tstr15 c
        where s.ban_key=c.ban_key
                and (c.TEST_IND <> 1
                        or c.TEST_IND IS NULL)
                and s.Domain='-'
        order by s.BAN_KEY,
            s.SUBS_KEY,
            s.MARKET_KEY
    ),
    JNRTRANS as (
        select /*+ use_hash(sq_dim aggtrans) leading(sq_dim) parallel(aggtrans,4) parallel(sq_dim,4)*/
            sq_dim.BAN_KEY,
            sq_dim.SUBS_KEY,
            sq_dim.MARKET_KEY,
            sq_dim.DW_STATUS,
            sq_dim.DOMAIN,
            sq_dim.CITY_KEY,
            sq_dim.ADDRESS_KEY,
            sq_dim.JUR_IND,
            sq_dim.JUR_KEY,
            sq_dim.FTTB_INFLOW_IND,
            sq_dim.IPTV_INFLOW_IND,
            sq_dim.FTTB_ACTIVATION_DATE,
            sq_dim.IPTV_ACTIVATION_DATE,
            sq_dim.FTTB_CHANNEL_KEY,
            sq_dim.IPTV_CHANNEL_KEY,
            aggtrans.DATE_KEY,
            aggtrans.LAST_FTTB_SERVICE_KEY,
            aggtrans.LAST_IPTV_SERVICE_KEY,
            aggtrans.FTTB_SERVICE_CONNECT_IND,
            aggtrans.IPTV_SERVICE_CONNECT_IND,
            aggtrans.FTTB_SERVICE_CONNECT_IND_PRED,
            aggtrans.IPTV_SERVICE_CONNECT_IND_PRED,
            aggtrans.IPTV_BOX_IND,
            aggtrans.MULTIROOM_IND,
            aggtrans.IPTV_BOX_RENT_IND,
            aggtrans.FTTB_TECH_SUPPORT_IND,
            aggtrans.BUNDLE_KEY
        from AGGTRANS_FLTR_SRT aggtrans,
            SQ_DIM_INAC_SUBSCRIBER sq_dim
        where aggtrans.BAN_KEY(+) = sq_dim.BAN_KEY
            and aggtrans.SUBS_KEY(+) = sq_dim.SUBS_KEY
            and aggtrans.MARKET_KEY(+) = sq_dim.MARKET_KEY
    ),
    EXP_REACT_IND_SRT as (
        select /*+ parallel(JNRTRANS,4)*/
            BAN_KEY,
            SUBS_KEY,
            DOMAIN,
            DATE_KEY,
            MARKET_KEY,
            DW_STATUS,
            CITY_KEY,
            ADDRESS_KEY,
            JUR_IND,
            JUR_KEY,
            FTTB_INFLOW_IND,
            IPTV_INFLOW_IND,
            FTTB_ACTIVATION_DATE,
            IPTV_ACTIVATION_DATE,
            FTTB_CHANNEL_KEY,
            IPTV_CHANNEL_KEY,
            --LAST_FTTB_SERVICE_KEY
            decode(LAST_FTTB_SERVICE_KEY,
                    NULL, '-99',
                    LAST_FTTB_SERVICE_KEY
                   ) as LAST_FTTB_SERVICE_KEY,
            --LAST_IPTV_SERVICE_KEY
            decode(LAST_IPTV_SERVICE_KEY,
                    NULL, '-99',
                    LAST_IPTV_SERVICE_KEY
                    ) as LAST_IPTV_SERVICE_KEY,
            --FTTB_SERVICE_CONNECT_IND
            decode(FTTB_SERVICE_CONNECT_IND,
                    NULL, 0,
                    FTTB_SERVICE_CONNECT_IND
                    ) as FTTB_SERVICE_CONNECT_IND,
            --IPTV_SERVICE_CONNECT_IND
            decode(IPTV_SERVICE_CONNECT_IND,
                    NULL, 0,
                    IPTV_SERVICE_CONNECT_IND
                    ) as IPTV_SERVICE_CONNECT_IND,
            --FTTB_REACT_IND
            case
                when FTTB_SERVICE_CONNECT_IND = 1
                    and FTTB_SERVICE_CONNECT_IND_PRED = 0
                    and FTTB_ACTIVATION_DATE< DATE_KEY
                    then
                1
                else
                0
            end as FTTB_REACT_IND,
            --IPTV_REACT_IND
            case
                when IPTV_SERVICE_CONNECT_IND = 1
                    and IPTV_SERVICE_CONNECT_IND_PRED = 0
                    and IPTV_ACTIVATION_DATE < DATE_KEY
                    then
                1
                else
                0
            end as IPTV_REACT_IND,
            SYSDATE as POPULATION_TIME,
            --IPTV_BOX_IND
            decode(IPTV_BOX_IND,
                    NULL, 0,
                    IPTV_BOX_IND
                    ) as IPTV_BOX_IND,
            --MULTIROOM_IND
            decode(MULTIROOM_IND,
                    NULL, 0,
                    MULTIROOM_IND
                    ) as MULTIROOM_IND,
            --IPTV_BOX_RENT_IND
            decode(IPTV_BOX_RENT_IND,
                    NULL, 0,
                    IPTV_BOX_RENT_IND
                    ) as IPTV_BOX_RENT_IND,
            --FTTB_TECH_SUPPORT_IND
            decode(FTTB_TECH_SUPPORT_IND,
                    NULL, 0,
                    FTTB_TECH_SUPPORT_IND
                    ) as FTTB_TECH_SUPPORT_IND,
            --BUNDLE_KEY
            decode(BUNDLE_KEY,
                    NULL, -99,
                    BUNDLE_KEY
                    ) as BUNDLE_KEY
        from JNRTRANS
        order by BAN_KEY
    ),

    SQ_DIM_INAC_TICKET as (
        select /*+ use_hash(c tpar) leading (tpar) parallel(tpar,4) parallel(c,4)*/
            tpar.BAN_KEY,
            tpar.TICKET_ID,
            tpar.PARAM_NAME,
            tpar.TICKET_ID_PARENT
        from
            --tpar
            (select /*+ parallel(a,4)*/
                TICKET_ID,
                PARAM_NAME,
                PN_RANK,
                TICKET_ID_PARENT,
                --address_key_m
                max(case
                        when param_name = 'MOVEADDRESS'
                            then
                        address_key
                    end) over (partition by ticket_id_parent) as address_key_m,
                --BAN_KEY
                first_value(ban_key) over (partition by ticket_id_parent
                                                        order by pn_rank,
                                                                ban_key DESC
                                                        ROWS BETWEEN
                                                            UNBOUNDED PRECEDING
                                                            AND UNBOUNDED FOLLOWING
                                        ) as BAN_KEY
            from
                (select /*+ use_hash(t p) leading(p) parallel(p,4) parallel(t,4)*/
                    p.TICKET_ID as TICKET_ID_PARENT,
                    t.TICKET_ID,
                    p.PARAM_NAME,
                    t.BAN_KEY,
                    t.ADDRESS_KEY,
                    --pn_rank
                    case
                        when p.param_name = 'MOVEADDRESS'
                            and t.ban_key <> -99
                            then
                        0
                        when p.param_name = 'MOVEADDRESS'
                            and t.ban_key = -99
                            then
                        2
                        else
                        1
                    end as PN_RANK
                from etl2_etl.DIM_INAC_TICKET@tstr15 t,
                    etl2_etl.FCT_INAC_TICKETS_PARAMS@tstr15 p
                where t.ticket_id = case
                                        when p.param_name = 'MOVEADDRESS'
                                            then
                                        p.ticket_id
                                        when p.param_name = 'RELATED'
                                            then p.param_number
                                    end
                    and p.param_name in ('MOVEADDRESS','RELATED')
                    and p.EFFECTIVE_DATE <= to_date('17.07.2017','DD.MM.YYYY')
                    AND p.EXPIRATION_DATE > to_date('17.07.2017','DD.MM.YYYY')
                    --условия на статус завершенности переезда и тип зависимой заявки
                    and ((p.param_name = 'MOVEADDRESS' and t.ticket_type_key=1 )
                        or(p.param_name='RELATED' and  t.ticket_type_key=234))

                ) a
            ) tpar,
            etl2_etl.DIM_INAC_CONTRACTS@tstr15 c
        where tpar.BAN_KEY=c.BAN_KEY
            and tpar.address_key_m = case
                                        when pn_rank = 2
                                            then
                                        c.adress_key   --проверяем совпадение адресов только в случае param_name='MOVEADDRESS' and ban_key=-99
                                        else
                                        tpar.address_key_m
                                    end
        ORDER BY tpar.ticket_id, tpar.param_name
    ),
    SQ_FCT_INAC_TICKET_ACTIVITY as (
        select /*+ parallel(a,4)*/
            ticket_id,
            --param_name
            case
                when curr_ticket_status_key = 4
                    then
                'MOVEADDRESS'
                else
                'RELATED'
            end as param_name,
            min(activity_date_time) as ACTIVITY_DATE_TIME
        from dwh_dwh.FCT_INAC_TICKET_ACTIVITY@tstr15 a
        where curr_ticket_status_key in (4, 81)
        group by ticket_id,
            curr_ticket_status_key
        order by ticket_id,
            case
                when curr_ticket_status_key = 4
                    then
                'MOVEADDRESS'
                else
                'RELATED'
            end
    ),
    JNR_TICKET_ACTIVITY as (--normal join = inner join
        select /*+ use_hash(sq_dim sq_fct) leading(sq_fct) parallel(sq_fct,4) parallel(sq_dim,4)*/
            sq_dim.BAN_KEY,
            sq_dim.PARAM_NAME,
            sq_dim.TICKET_ID_PARENT,
            sq_fct.ACTIVITY_DATE_TIME
        from SQ_DIM_INAC_TICKET sq_dim,
            SQ_FCT_INAC_TICKET_ACTIVITY sq_fct
        where sq_dim.TICKET_ID = sq_fct.TICKET_ID
            AND sq_dim.PARAM_NAME = sq_fct.PARAM_NAME
    ),
    AGG_TICKET_ID_PARENT_FLT as (
        select /*+ parallel(jrn,4)*/
            TICKET_ID_PARENT,
            BAN_KEY,
            max(decode(PARAM_NAME,
                    'MOVEADDRESS', ACTIVITY_DATE_TIME,
                    null)
                ) as DATE_FTTB_MOVEADDRESS,
            count(PARAM_NAME) as TICKET_COUNT
        from JNR_TICKET_ACTIVITY jnr
        group by TICKET_ID_PARENT,
                BAN_KEY
        having count(PARAM_NAME) = 2
    ),
    AGG_LST_DT_FTTB_MVEADDRSS_SRT as (
        select /*+ parallel(AGG_TICKET_ID_PARENT_FLT,4)*/
            BAN_KEY,
            max(DATE_FTTB_MOVEADDRESS) as DATE_FTTB_MOVEADDRESS
        from AGG_TICKET_ID_PARENT_FLT
        group by BAN_KEY
        order by BAN_KEY
    ),

    SQ_DIM_BUSSINES as (
        select /*+ use_hash(dbs dbst) leading(dbst) parallel(dbs,4) parallel(dbst,4)*/
            dbs.BUSINESS_SERVICE_KEY,
            dbst.BUSINESS_SERVICE_TYPE_KEY
        from dwh_dwh.DIM_BUSINESS_SERVICE@tstr15 dbs,
            dwh_dwh.DIM_BUSINESS_SERVICE_TYPE@tstr15 dbst
        where dbs.BUSINESS_SERVICE_TYPE_KEY = dbst.BUSINESS_SERVICE_TYPE_KEY
            and dbst.revenue_stream <> '34'
            and (dbst.BUSINESS_SERVICE_TYPE_KEY = '255' --$$BST_FTTB=255
                OR dbst.BUSINESS_SERVICE_TYPE_KEY = '259' --$$BST_IPTV=259
                )
        order by dbs.BUSINESS_SERVICE_KEY
    ),
    SQ_FCT_INAC_USAGE as (
        select /*+ parallel(fiu,4)*/
            fiu.BAN_KEY,
            fiu.SUBS_KEY,
            fiu.MARKET_KEY,
            fiu.BUSINESS_SERVICE_KEY,
            --DATA_VOLUME
            DECODE(SIGN(nvl(fiu.UPLOAD_DATA_VOLUME,0) + nvl(fiu.DOWNLOAD_DATA_VOLUME,0)),
                        1, nvl(fiu.UPLOAD_DATA_VOLUME,0) + nvl(fiu.DOWNLOAD_DATA_VOLUME,0),
                        0
                    ) as DATA_VOLUME,
            --REVENUE_RUR
            case
                when fiu.CURRENCY_KEY = 'P'
                    then
                nvl(fiu.CHARGE_AMT,0) + nvl(fiu.VAT_AMT,0)
                else
                (nvl(fiu.CHARGE_AMT,0)+ nvl(fiu.VAT_AMT,0)) * fiu.CURRENCY_RATE
            END AS REVENUE_RUR
        from dwh_dwh.FCT_INAC_USAGE@tstr15 fiu
        where (fiu.report_date_key < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                AND fiu.report_date_key >= TO_DATE('17.07.2017','DD.MM.YYYY'))
          and fiu.BUSINESS_SERVICE_KEY <> '-99'
          and fiu.REVENUE_CODE <> 'N'
        order by fiu.BUSINESS_SERVICE_KEY
    ),
    SQ_FCT_INAC_CHARGES as (
        select /*+ parallel(fic,4)*/
            fic.BAN_KEY,
            fic.SUBS_KEY,
            fic.MARKET_KEY,
            fic.BUSINESS_SERVICE_KEY,
            --REVENUE_RUR
            case
                when fic.CURRENCY_KEY = 'P'
                    then
                nvl(fic.CHARGE_AMT,0) + nvl(fic.VAT_AMT,0)
                else
                (nvl(fic.CHARGE_AMT,0) + nvl(fic.VAT_AMT,0)) * fic.CURRENCY_RATE
            end as REVENUE_RUR
        from dwh_dwh.FCT_INAC_CHARGES@tstr15 fic
        where (fic.report_date_key < TO_DATE('17.07.2017','DD.MM.YYYY') + 1
                AND fic.report_date_key >= TO_DATE('17.07.2017','DD.MM.YYYY'))
            and fic.BUSINESS_SERVICE_KEY <> '-99'
            and fic.REVENUE_CODE <> 'N'
        order by fic.BUSINESS_SERVICE_KEY
    ),
    JRN_SQ_INAC_USG_left_SQ_DIM as (
        select /*+ use_hash(sfiu sdb) leading(dbst) parallel(sfiu,4) parallel(sdb,4)*/
            sfiu.BAN_KEY,
            sfiu.SUBS_KEY,
            sfiu.MARKET_KEY,
            sfiu.DATA_VOLUME,
            sfiu.REVENUE_RUR,
            sdb.BUSINESS_SERVICE_TYPE_KEY
        from SQ_DIM_BUSSINES sdb,
            SQ_FCT_INAC_USAGE sfiu
        where sfiu.BUSINESS_SERVICE_KEY = sdb.BUSINESS_SERVICE_KEY(+)
    ),
    JNR_SQ_INAC_CHRGS_left_SQ_DIM as (
        select /*+ use_hash(sfi sdb) leading(dbst) parallel(sfi,4) parallel(sdb,4)*/
            sdb.BUSINESS_SERVICE_TYPE_KEY,
            sfi.BAN_KEY,
            sfi.SUBS_KEY,
            sfi.MARKET_KEY,
            sfi.REVENUE_RUR
        from SQ_FCT_INAC_CHARGES sfi,
            SQ_DIM_BUSSINES sdb
        where sfi.BUSINESS_SERVICE_KEY = sdb.BUSINESS_SERVICE_KEY(+)
    ),
    UNION_2JRN_SRT as (
        select
            BAN_KEY,
            SUBS_KEY,
            MARKET_KEY,
            DATA_VOLUME,
            REVENUE_RUR,
            BUSINESS_SERVICE_TYPE_KEY
        from
            (select /*+ parallel(a,4)*/
                a.BAN_KEY,
                a.SUBS_KEY,
                a.MARKET_KEY,
                cast((select null as DATA_VOLUME from dual) as number(15,2)) as DATA_VOLUME,
                a.REVENUE_RUR,
                a.BUSINESS_SERVICE_TYPE_KEY
            from JNR_SQ_INAC_CHRGS_left_SQ_DIM a
            union all
            select /*+ parallel(b,4)*/
                b.BAN_KEY,
                b.SUBS_KEY,
                b.MARKET_KEY,
                b.DATA_VOLUME,
                b.REVENUE_RUR,
                b.BUSINESS_SERVICE_TYPE_KEY
            from JRN_SQ_INAC_USG_left_SQ_DIM b)
        order by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
    ),
    AGG_DATA_REVENUE_SRT as (
        select /*+ parallel(un,4)*/
            BAN_KEY,
            SUBS_KEY,
            MARKET_KEY,
            --FTTB_DATA_VOLUME
            SUM(decode(BUSINESS_SERVICE_TYPE_KEY,
                        TO_NUMBER('255'), DATA_VOLUME, --$$BST_FTTB=255
                        0)
                ) as FTTB_DATA_VOLUME,
            --FTTB_REVENUE_RUR
            SUM(decode(BUSINESS_SERVICE_TYPE_KEY,
                        TO_NUMBER('255'), REVENUE_RUR, --$$BST_FTTB=255
                        0)
                ) as FTTB_REVENUE_RUR,
            --IPTV_REVENUE_RUR
            SUM(decode(BUSINESS_SERVICE_TYPE_KEY,
                        TO_NUMBER('259'), REVENUE_RUR, --$$BST_IPTV=259
                        0)
                ) as IPTV_REVENUE_RUR
        from UNION_2JRN_SRT un
        group by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
        order by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
    ),

    SQ_DIM_INAC_SB_CNVRGNT as (
        select /*+ parallel(source,4)*/
            FTTB_BAN_KEY as BAN_KEY,
            FTTB_SUBS_KEY as SUBS_KEY,
            FTTB_MARKET_KEY as MARKET_KEY,
            LAST_VALUE(CNVRG_SEGMENT_KEY IGNORE NULLS) over (partition by FTTB_BAN_KEY,
                                                                            FTTB_SUBS_KEY,
                                                                            FTTB_MARKET_KEY
                                                                            order by EFFECTIVE_DATE,
                                                                                    EXPIRATION_DATE
                                                            ) as CNVRG_SEGMENT_KEY
        from (
            select /*+ parallel(CNVRG,4)*/
                CNVRG.FTTB_BAN_KEY,
                CNVRG.FTTB_SUBS_KEY,
                CNVRG.FTTB_MARKET_KEY,
                CNVRG.CNVRG_SEGMENT_KEY,
                CNVRG.EFFECTIVE_DATE,
                CNVRG.EXPIRATION_DATE
            from etl2_etl.DIM_INAC_SUBS_CONVERGENT@tstr15 CNVRG
            where CNVRG.cnvrg_segment_key <>'5'
                and CNVRG.EFFECTIVE_DATE < to_date('17.07.2017', 'dd.mm.yyyy') + 1
                and CNVRG.EXPIRATION_DATE >= to_date('17.07.2017', 'dd.mm.yyyy')
            ) source
        order by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
    ),
    AGG_CONVERGENT_SRT as (
        select /*+ parallel(SQ_DIM_INAC,4)*/
            BAN_KEY,
            SUBS_KEY,
            MARKET_KEY,
            max(CNVRG_SEGMENT_KEY) as CNVRG_SEGMENT_KEY
        from SQ_DIM_INAC_SB_CNVRGNT
        group by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
        order by BAN_KEY,
                SUBS_KEY,
                MARKET_KEY
    ),

    JNR_DATE_MOVEADDRESS_SRT as (
        select
            ext.BAN_KEY,
            ext.SUBS_KEY,
            ext.DOMAIN,
            ext.DATE_KEY,
            ext.MARKET_KEY,
            ext.DW_STATUS,
            ext.CITY_KEY,
            ext.ADDRESS_KEY,
            ext.JUR_IND,
            ext.JUR_KEY,
            ext.FTTB_INFLOW_IND,
            ext.IPTV_INFLOW_IND,
            ext.FTTB_ACTIVATION_DATE,
            ext.IPTV_ACTIVATION_DATE,
            ext.FTTB_CHANNEL_KEY,
            ext.IPTV_CHANNEL_KEY,
            ext.LAST_FTTB_SERVICE_KEY,
            ext.LAST_IPTV_SERVICE_KEY,
            ext.FTTB_SERVICE_CONNECT_IND,
            ext.IPTV_SERVICE_CONNECT_IND,
            ext.FTTB_REACT_IND,
            ext.IPTV_REACT_IND,
            ext.IPTV_BOX_IND,
            ext.MULTIROOM_IND,
            ext.IPTV_BOX_RENT_IND,
            ext.FTTB_TECH_SUPPORT_IND,
            ext.BUNDLE_KEY,
            agg.DATE_FTTB_MOVEADDRESS
        from AGG_LST_DT_FTTB_MVEADDRSS_SRT agg,
            EXP_REACT_IND_SRT ext
        where ext.BAN_KEY = agg.BAN_KEY(+)
        order by ext.BAN_KEY,
            ext.SUBS_KEY,
            ext.MARKET_KEY
    ),

    JNR_DATA_REVENUE as (
        select /*+ use_hash(agg jnr) leading(agg) parallel(agg,4) parallel(jnr,4)*/
            jnr.BAN_KEY,
            jnr.SUBS_KEY,
            jnr.DOMAIN,
            jnr.DATE_KEY,
            jnr.MARKET_KEY,
            jnr.DW_STATUS,
            jnr.CITY_KEY,
            jnr.ADDRESS_KEY,
            jnr.JUR_IND,
            jnr.JUR_KEY,
            jnr.FTTB_INFLOW_IND,
            jnr.IPTV_INFLOW_IND,
            jnr.FTTB_ACTIVATION_DATE,
            jnr.IPTV_ACTIVATION_DATE,
            jnr.FTTB_CHANNEL_KEY,
            jnr.IPTV_CHANNEL_KEY,
            jnr.LAST_FTTB_SERVICE_KEY,
            jnr.LAST_IPTV_SERVICE_KEY,
            jnr.FTTB_SERVICE_CONNECT_IND,
            jnr.IPTV_SERVICE_CONNECT_IND,
            jnr.FTTB_REACT_IND,
            jnr.IPTV_REACT_IND,
            jnr.IPTV_BOX_IND,
            jnr.MULTIROOM_IND,
            jnr.IPTV_BOX_RENT_IND,
            jnr.FTTB_TECH_SUPPORT_IND,
            jnr.BUNDLE_KEY,
            jnr.DATE_FTTB_MOVEADDRESS,
            agg.FTTB_DATA_VOLUME,
            agg.FTTB_REVENUE_RUR,
            agg.IPTV_REVENUE_RUR
        from AGG_DATA_REVENUE_SRT agg,
            JNR_DATE_MOVEADDRESS_SRT jnr
        where agg.BAN_KEY(+) = jnr.BAN_KEY
            AND agg.SUBS_KEY(+) = jnr.SUBS_KEY
            AND agg.MARKET_KEY(+) = jnr.MARKET_KEY
    ),

    EXP_DATA_REVENUE_SRT as (
        select /*+ parallel(JNR_DATA_REVENUE,4)*/
            BAN_KEY,
            SUBS_KEY,
            DOMAIN,
            DATE_KEY,
            MARKET_KEY,
            DW_STATUS,
            CITY_KEY,
            ADDRESS_KEY,
            JUR_IND,
            JUR_KEY,
            FTTB_INFLOW_IND,
            IPTV_INFLOW_IND,
            FTTB_ACTIVATION_DATE,
            IPTV_ACTIVATION_DATE,
            FTTB_CHANNEL_KEY,
            IPTV_CHANNEL_KEY,
            LAST_FTTB_SERVICE_KEY,
            LAST_IPTV_SERVICE_KEY,
            FTTB_SERVICE_CONNECT_IND,
            IPTV_SERVICE_CONNECT_IND,
            FTTB_REACT_IND,
            IPTV_REACT_IND,
            IPTV_BOX_IND,
            MULTIROOM_IND,
            IPTV_BOX_RENT_IND,
            FTTB_TECH_SUPPORT_IND,
            BUNDLE_KEY,
            DATE_FTTB_MOVEADDRESS,
            --FTTB_DATA_VOLUME
            decode(FTTB_DATA_VOLUME,
                    null, 0,
                    FTTB_DATA_VOLUME) as FTTB_DATA_VOLUME,
            --FTTB_REVENUE_RUR
            decode(FTTB_REVENUE_RUR,
                    null, 0,
                    FTTB_REVENUE_RUR) as FTTB_REVENUE_RUR,
            --IPTV_REVENUE_RUR
            decode(IPTV_REVENUE_RUR,
                    null, 0,
                    IPTV_REVENUE_RUR) as IPTV_REVENUE_RUR
        from JNR_DATA_REVENUE
        order by BAN_KEY,
            SUBS_KEY,
            MARKET_KEY
    ),

    JNR_CONVERGENT as (
        select /*+ use_hash(expr agg) leading(agg) parallel(agg,4) parallel(expr,4)*/
            expr.BAN_KEY,
			expr.SUBS_KEY,
			expr.DOMAIN,
			expr.DATE_KEY,
			expr.MARKET_KEY,
			expr.DW_STATUS,
			expr.CITY_KEY,
			expr.ADDRESS_KEY,
			expr.JUR_IND,
			expr.JUR_KEY,
			expr.FTTB_INFLOW_IND,
			expr.IPTV_INFLOW_IND,
			expr.FTTB_ACTIVATION_DATE,
			expr.IPTV_ACTIVATION_DATE,
			expr.FTTB_CHANNEL_KEY,
			expr.IPTV_CHANNEL_KEY,
			expr.LAST_FTTB_SERVICE_KEY,
			expr.LAST_IPTV_SERVICE_KEY,
			expr.FTTB_SERVICE_CONNECT_IND,
			expr.IPTV_SERVICE_CONNECT_IND,
			expr.FTTB_REACT_IND,
			expr.IPTV_REACT_IND,
			expr.IPTV_BOX_IND,
			expr.MULTIROOM_IND,
			expr.IPTV_BOX_RENT_IND,
			expr.FTTB_TECH_SUPPORT_IND,
			expr.BUNDLE_KEY,
			expr.DATE_FTTB_MOVEADDRESS,
			expr.FTTB_DATA_VOLUME,
			expr.FTTB_REVENUE_RUR,
			expr.IPTV_REVENUE_RUR,
            agg.CNVRG_SEGMENT_KEY
        from EXP_DATA_REVENUE_SRT expr,
            AGG_CONVERGENT_SRT agg
        where expr.BAN_KEY = agg.BAN_KEY(+)
            AND expr.SUBS_KEY = agg.SUBS_KEY(+)
            AND expr.MARKET_KEY = agg.MARKET_KEY(+)
    ),

    et as (
        select /*+ parallel(a,4)*/
            BAN_KEY,
            SUBS_KEY,
            DOMAIN,
            DATE_KEY,
            MARKET_KEY,
            DW_STATUS,
            CITY_KEY,
            ADDRESS_KEY,
            JUR_IND,
            JUR_KEY,
            FTTB_INFLOW_IND,
            IPTV_INFLOW_IND,
            FTTB_ACTIVATION_DATE,
            IPTV_ACTIVATION_DATE,
            FTTB_CHANNEL_KEY,
            IPTV_CHANNEL_KEY,
            LAST_FTTB_SERVICE_KEY,
            LAST_IPTV_SERVICE_KEY,
            FTTB_SERVICE_CONNECT_IND,
            IPTV_SERVICE_CONNECT_IND,
            FTTB_REACT_IND,
            IPTV_REACT_IND,
            IPTV_BOX_IND,
            MULTIROOM_IND,
            IPTV_BOX_RENT_IND,
            FTTB_TECH_SUPPORT_IND,
            BUNDLE_KEY,
            DATE_FTTB_MOVEADDRESS,
            FTTB_DATA_VOLUME,
            FTTB_REVENUE_RUR,
            IPTV_REVENUE_RUR,
            CNVRG_SEGMENT_KEY
        from etl2_etl.AGG_AAB_FTTB_DAILY_I@tstr15 a
    )


select count(*) from JNR_CONVERGENT
union all
select count(*) from et
;
