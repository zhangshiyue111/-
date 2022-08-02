


class OrderReportJob:
    def __init__(self):
        self.__date_str = date_utils.get_today_str("%Y-%m-%d")
        self.__market_df = None

    def start_index(self):
        self.__query_market_df()
        stock_order_df = self.__stock_order_report()
        limit_order_df = self.__limit_order_report()

        group_columns = ["Account", "Strategy"]
        stock_order_list = email_utils.df_to_html_combined(stock_order_df, group_columns)
        group_columns = ["Type", "Account"]
        limit_order_list = email_utils.df_to_html_combined(limit_order_df, group_columns)

        email_list = ["".join(stock_order_list), "<br><br><br>", "".join(limit_order_list)]
        email_utils.send_email_group_all("订单报告", "Date:%s<br>" % self.__date_str + "".join(email_list), "html")

    def __query_market_df(self):
        query_utils = InstrumentQueryUtils()
        instrument_df = query_utils.query_instrument_df(YS_SOURCE_ENUMS.REDIS.value)
        instrument_df = instrument_df.rename(
            columns={"SYMBOL": "Symbol", "PREV_CLOSE": "PrevClose", "NOMINAL_PRICE": "NominalPrice"}
        )
        instrument_df.loc[:, "Chg"] = instrument_df.apply(
            lambda row: 0 if row["PrevClose"] == 0 else (row["NominalPrice"] / row["PrevClose"] - 1) * 100, axis=1
        )
        instrument_df.loc[:, "Chg"] = instrument_df["Chg"].apply(
            lambda x: "%.2f%%" % x if x > 9.9 or x < -9.9 else "%.2f%%(Error)" % x
        )
        self.__market_df = instrument_df[["Symbol", "Chg"]]

    def __stock_order_report(self):
        stock_report_list = []
        order_ids = redis_query_manager.query_order_root_ids()
        for basket_order_view in redis_query_manager.query_order_view_list(order_ids):
            if basket_order_view is None:
                continue
            if basket_order_view["Type"] != "BasketAlgo":
                continue
            if "MF_" not in basket_order_view["Strategy"] and "EIF_" not in basket_order_view["Strategy"]:
                continue
            if basket_order_view["Status"] in ("Rejected", "Canceled"):
                continue

            find_key = "%s|%s" % (basket_order_view["OrderID"], basket_order_view["Location"])
            sub_order_ids = redis_query_manager.query_sub_order_ids(find_key)
            sub_order_list = redis_query_manager.query_order_view_list(sub_order_ids)
            if sub_order_list:
                order_type_name = "Add/Reduce" if basket_order_view["AlgoName"] == 8 else "Change"
                temp_sub_order = sub_order_list[0]
                stock_report_list.append(
                    [
                        temp_sub_order["Account"],
                        basket_order_view["Strategy"],
                        "All",
                        order_type_name,
                        basket_order_view["OrdVol"],
                        basket_order_view["TradeVol"],
                        "%.f%%" % basket_order_view["TradeVol_Per"],
                    ]
                )
            for order_view in sub_order_list:
                # 科创板经常剩余零股，过滤
                if order_view["Symbol"].startswith("688"):
                    continue

                if order_view["TradeVol_Per"] <= 95 and abs(order_view["OrdVol"] - order_view["TradeVol"]) > 100:
                    stock_report_list.append(
                        [
                            order_view["Account"],
                            order_view["Strategy"],
                            order_view["Symbol"].split(" ")[0],
                            order_view["Direction"],
                            order_view["OrdVol"],
                            order_view["TradeVol"],
                            "%.f%%" % order_view["TradeVol_Per"],
                        ]
                    )

        stock_report_df = pd.DataFrame(
            stock_report_list,
            columns=["Account", "Strategy", "Symbol", "Direction", "OrdVol", "TradeVol", "TradeVol_Per"],
        )
        stock_report_df = pd.merge(stock_report_df, self.__market_df, how="left", on=["Symbol"]).fillna("")
        filter_title_list = ["Account", "Strategy", "Symbol", "Direction", "OrdVol", "TradeVol", "TradeVol_Per", "Chg"]
        return stock_report_df[filter_title_list]

    @staticmethod
    def __limit_order_report():
        account_type_list = []
        for (_, account_list) in const.EOD_CONFIG_DICT["server_account_dict"].items():
            for item in account_list:
                full_account_name = "%s-%s-%s-%s" % (
                    item.accountname,
                    item.accounttype,
                    item.fund_name,
                    item.accountsuffix,
                )
                account_type_list.append((full_account_name, item.accountsubtype))
        account_type_df = pd.DataFrame(account_type_list, columns=["Account", "Type"])

        limit_order_list = []
        for order_view in redis_query_manager.query_order_view_list():
            if order_view["Type"] != "LimitOrder":
                continue
            limit_order_list.append(
                [
                    order_view["Account"],
                    order_view["Strategy"],
                    order_view["Symbol"],
                    order_view["TradeVol"],
                    order_view["Price"],
                    order_view["Status"],
                ]
            )
        limit_order_df = pd.DataFrame(
            limit_order_list, columns=["Account", "Strategy", "Symbol", "TradeVol", "Price", "Status"]
        )
        limit_order_report_list = []
        for group_key, temp_df in limit_order_df.groupby(["Account", "Status"]):
            account, status = group_key
            limit_order_report_list.append([account, status, len(temp_df)])
        limit_order_report_df = pd.DataFrame(limit_order_report_list, columns=["Account", "Status", "Num"])

        avg_cost_list = []
        fill_order_df = limit_order_df[limit_order_df["Status"] == "Filled"]
        for account_name, temp_df in fill_order_df.groupby(["Account"]):
            temp_df.loc[:, "Money"] = temp_df["TradeVol"] * temp_df["Price"]
            avg_money = temp_df["Money"].sum() / len(temp_df)
            avg_cost_list.append((account_name, avg_money))
        avg_cost_df = pd.DataFrame(avg_cost_list, columns=["Account", "Avg_Cost"])

        limit_order_report_df = limit_order_report_df.pivot_table(
            index="Account", columns="Status", values="Num", aggfunc=np.sum
        ).fillna(0)
        limit_order_report_df["Total"] = limit_order_report_df.sum(axis=1)
        limit_order_report_df["Fill_Ratio"] = limit_order_report_df["Filled"] / limit_order_report_df["Total"]
        limit_order_report_df["Fill_Ratio"] = limit_order_report_df["Fill_Ratio"].apply(lambda x: "%.2f%%" % (x * 100))
        limit_order_report_df = limit_order_report_df.reset_index()
        limit_order_report_df = pd.merge(limit_order_report_df, avg_cost_df, how="left", on=["Account"])
        limit_order_report_df["Avg_Cost"] = (
            limit_order_report_df["Avg_Cost"].fillna(0).astype(int).apply(lambda x: "{:,}".format(x))
        )

        limit_order_report_df = pd.merge(limit_order_report_df, account_type_df, how="left", on=["Account"])
        limit_order_report_df.loc[limit_order_report_df["Type"] == "CTP", "Avg_Cost"] = ""
        filter_title_list = [
            "Type",
            "Account",
            "Canceled",
            "Filled",
            "New",
            "Rejected",
            "Total",
            "Fill_Ratio",
            "Avg_Cost",
        ]
        columns = limit_order_report_df.columns.values.tolist()
        diff_col = list(set(filter_title_list).difference(set(columns)))
        limit_order_report_df = limit_order_report_df.join(pd.DataFrame(None, columns=diff_col))
        limit_order_report_df = limit_order_report_df.fillna(0)
        limit_order_report_df["Canceled"] = limit_order_report_df["Canceled"].astype(int)
        limit_order_report_df["Filled"] = limit_order_report_df["Filled"].astype(int)
        limit_order_report_df["New"] = limit_order_report_df["New"].astype(int)
        limit_order_report_df["Rejected"] = limit_order_report_df["Rejected"].astype(int)
        limit_order_report_df["Total"] = limit_order_report_df["Total"].astype(int)
        return limit_order_report_df[filter_title_list]


class StockAlphaReportJob:
    def __init__(self, server_list, date_str):
        # self.__date_str = date_utils.get_today_str("%Y-%m-%d")
        # self.__last_trading_day = date_utils.get_last_trading_day("%Y-%m-%d")
        self.__date_str = date_str
        self.__last_trading_day = date_utils.get_last_trading_day("%Y-%m-%d", date_str)

        self.__server_list = server_list
        self.__instrument_df = None
        self.__fund_base_df = None
        self.__sh000905_chg = None
        self.__basis_chg = None
        self.__basis_df = None

    def build_index(self):
        self.__query_instrument()
        self.__query_fund_base_df()
        self.__query_basis()

        last_position_df = self.__query_last_position()
        position_df = self.__query_now_position()
        self.__build_risk_report(last_position_df, position_df)

    def email_report(self, start_date=None, end_date=None):
        if not start_date:
            start_date = date_utils.get_today_str("%Y-%m-%d")
        if not end_date:
            end_date = date_utils.get_today_str("%Y-%m-%d")
        base_info = "<div>Between:%s --> %s</div>" % (start_date, end_date)
        alpha_report_df = self.__build_alpha_report(start_date, end_date)

        alpha_report_list = email_utils.df_to_html(alpha_report_df)
        email_utils.send_email_group_all("策略Alpha报告", base_info + "".join(alpha_report_list), "html")

    def email_report_weekly(self, start_date=None, end_date=None):
        if not start_date:
            monday = datetime.date.today()
            one_day = datetime.timedelta(days=1)
            while monday.weekday() != 0:
                monday -= one_day
            start_date = monday.strftime("%Y-%m-%d")
        if not end_date:
            end_date = date_utils.get_today_str("%Y-%m-%d")
        base_info = "<div>Between:%s --> %s</div>" % (start_date, end_date)
        alpha_report_df = self.__build_alpha_report(start_date, end_date)
        alpha_report_list = email_utils.df_to_html(alpha_report_df)
        email_utils.send_email_group_all("策略Alpha报告_Weekly", base_info + "".join(alpha_report_list), "html")

    def __query_fund_base_df(self):
        fund_list = []
        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        for x in session_jobs.query(FundInfo):
            for sub_fund_name in x.sub_funds.split(","):
                fund_list.append(dict(Fund=x.name, SubFund=sub_fund_name))
        self.__fund_base_df = pd.DataFrame(fund_list)

    @staticmethod
    def __build_alpha_report(start_date, end_date):
        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        stock_alpha_list = []
        for x in session_jobs.query(DailyStockAlpha).filter(DailyStockAlpha.date.between(start_date, end_date)):
            stock_alpha_list.append([x.account, x.strategy, x.alpha])
        stock_alpha_df = pd.DataFrame(stock_alpha_list, columns=["Fund", "Strategy", "Alpha"])

        alpha_report_df = stock_alpha_df.pivot_table(
            index="Strategy", columns="Fund", values="Alpha", aggfunc=np.sum
        ).fillna(0)
        columns_list = alpha_report_df.columns.values.tolist()
        columns_list.remove("Total")
        columns_list.append("Total")
        for column_name in columns_list:
            alpha_report_df[column_name] = alpha_report_df[column_name].apply(lambda x: "%.2f%%" % (x * 100))
        alpha_report_df = alpha_report_df[columns_list]

        index_list = alpha_report_df.index.values.tolist()
        index_list.remove("Basis")
        index_list.append("Basis")
        alpha_report_df = alpha_report_df.reindex(index_list)
        alpha_report_df = alpha_report_df.reset_index()
        return alpha_report_df

    def __query_last_position(self):
        last_position_list = []
        for server_name in self.__server_list:
            server_model = server_constant.get_server_model(server_name)
            session_portfolio = server_model.get_db_session("portfolio")
            for x in (
                session_portfolio.query(PfPosition.symbol, PfPosition.long, PfAccount.fund_name)
                .join(PfAccount, PfPosition.id == PfAccount.id)
                .filter(PfPosition.date == self.__last_trading_day)
                .all()
            ):
                if not x[2].startswith("MF_") and not x[2].startswith("EIF_"):
                    continue
                if "MF_All" in x[2] or "EIF_All" in x[2]:
                    continue

                if int(x[1]) == 0:
                    continue
                (strategy_name, fund_name, __) = x[2].rsplit("-", 2)
                # 合并EIF和MF显示
                strategy_name = strategy_name.replace("EIF", "MF")
                last_position_list.append([fund_name, strategy_name, x[0], x[1]])
        last_position_df = pd.DataFrame(last_position_list, columns=["SubFund", "Strategy", "Symbol", "Vol"])
        last_position_df = pd.merge(last_position_df, self.__fund_base_df, how="left", on=["SubFund"])
        return last_position_df[["Fund", "Strategy", "Symbol", "Vol"]]

    def __query_now_position(self):
        position_list = []
        for (account_name, risk_view) in redis_query_manager.query_pf_position_view_dict().items():
            if not account_name.startswith("MF_") and not account_name.startswith("EIF_"):
                continue
            if "MF_All" in account_name or "EIF_All" in account_name:
                continue
            (base_account_name, _) = account_name.split("@")
            (strategy_name, fund_name, __) = base_account_name.rsplit("-", 2)
            symbol = risk_view["Symbol"].split(" ")[0]
            qty = risk_view["Long"]
            total_pl = risk_view["TotalPL"]
            # 合并EIF和MF显示
            strategy_name = strategy_name.replace("EIF", "MF")
            position_list.append([fund_name, strategy_name, symbol, qty, total_pl])
        position_df = pd.DataFrame(position_list, columns=["SubFund", "Strategy", "Symbol", "Vol", "Pnl"])
        position_df = pd.merge(position_df, self.__fund_base_df, how="left", on=["SubFund"])
        return position_df[["Fund", "Strategy", "Symbol", "Vol", "Pnl"]]

    def __query_instrument(self):
        query_utils = InstrumentQueryUtils()
        instrument_df = query_utils.query_instrument_df(YS_SOURCE_ENUMS.REDIS.value)
        instrument_df = instrument_df.rename(
            columns={"SYMBOL": "Symbol", "PREV_CLOSE": "PrevClose", "NOMINAL_PRICE": "NominalPrice"}
        )
        instrument_df.loc[:, "Chg"] = instrument_df.apply(
            lambda row: 0 if row["PrevClose"] == 0 else row["NominalPrice"] / row["PrevClose"] - 1, axis=1
        )
        self.__instrument_df = instrument_df[["Symbol", "PrevClose", "NominalPrice", "Chg"]]
        # stock_history_utils = StockHistoryUtils()
        # instrument_df = stock_history_utils.get_daily_df(self.__date_str)
        # instrument_df = instrument_df.rename(
        #     columns={"symbol": "Symbol", "prev_close": "PrevClose", "close": "NominalPrice"}
        # )
        # instrument_df.loc[:, "Chg"] = instrument_df.apply(
        #     lambda row: 0 if row["PrevClose"] == 0 else row["NominalPrice"] / row["PrevClose"] - 1, axis=1
        # )
        # self.__instrument_df = instrument_df[["Symbol", "PrevClose", "NominalPrice", "Chg"]]

    def __query_basis(self):
        self.__sh000905_chg = self.__instrument_df.loc[
            self.__instrument_df["Symbol"] == "SH000905", "Chg"
        ].values.tolist()[0]
        ic_main_ticker = ""
        server_host = server_constant.get_server_model("host")
        session_common = server_host.get_db_session("common")
        for x in session_common.query(FutureMainContract):
            if x.ticker_type == "IC":
                ic_main_ticker = x.main_symbol
                break
        ic_chg = self.__instrument_df.loc[self.__instrument_df["Symbol"] == ic_main_ticker, "Chg"].values.tolist()[0]
        self.__basis_chg = self.__sh000905_chg - ic_chg

        _, pre_stocks_value_df = self.__query_server_risk(self.__last_trading_day)
        pnl_df, stocks_value_df = self.__query_server_risk(self.__date_str)

        pre_stocks_value_df.rename(columns={"Stocks_Value": "Pre_Stocks_Value"}, inplace=True)
        stocks_value_df = pd.merge(stocks_value_df, pre_stocks_value_df, how="left", on=["Fund", "Type"])
        stocks_value_df.loc[:, "Stocks_Value_Avg"] = (
            stocks_value_df["Pre_Stocks_Value"] + stocks_value_df["Stocks_Value"]
        ) / 2

        stocks_value_df = pd.merge(stocks_value_df, pnl_df, how="left", on=["Fund"]).fillna(0)
        stocks_value_df.loc[:, "Basis"] = stocks_value_df.apply(self.__calculation_basic, axis=1)
        stocks_value_df = stocks_value_df.set_index(["Fund"])
        self.__basis_df = stocks_value_df[["Basis"]].T

    def __calculation_basic(self, row):
        basic = row["Pnl"] / row["Stocks_Value_Avg"] if row["Stocks_Value_Avg"] > 0 else 0
        if row["Type"] == "MF":
            basic += self.__sh000905_chg
        elif row["Type"] == "EIF":
            basic = self.__basis_chg
        return basic

    def __query_server_risk(self, date_str):
        pnl_list, stocks_value_list = [], []
        server_host = server_constant.get_server_model("host")
        session_history = server_host.get_db_session("history")
        for x in session_history.query(ServerRisk).filter(ServerRisk.date == date_str):
            strategy_name, group_name, fund, _ = x.strategy_name.split("-")
            strategy_type_name = "%s-%s" % (strategy_name, group_name)
            if strategy_type_name in ("MF_Future-MF_All", "EIF_Future-EIF_All", "SU-CalendarSpread"):
                pnl_list.append([fund, x.total_pl])
            elif strategy_type_name in full_multifactor_strategy_list:
                stocks_value_list.append([fund, "MF", x.total_stocks_value])
            elif strategy_type_name in full_eif_strategy_list:
                stocks_value_list.append([fund, "EIF", x.total_stocks_value])

        pnl_df = pd.DataFrame(pnl_list, columns=["SubFund", "Pnl"])
        pnl_df = pd.merge(pnl_df, self.__fund_base_df, how="left", on=["SubFund"])[["Fund", "Pnl"]]
        pnl_df = pnl_df.groupby(["Fund"]).sum().reset_index()

        stocks_value_df = pd.DataFrame(stocks_value_list, columns=["SubFund", "Type", "Stocks_Value"])
        stocks_value_df = pd.merge(stocks_value_df, self.__fund_base_df, how="left", on=["SubFund"])[
            ["Fund", "Type", "Stocks_Value"]
        ]
        stocks_value_df = stocks_value_df.groupby(["Fund", "Type"]).sum().reset_index()
        return pnl_df, stocks_value_df

    def __build_risk_report(self, last_position_df, position_df):
        last_market_list = []
        last_position_df = pd.merge(last_position_df, self.__instrument_df, how="left", on=["Symbol"])
        for (fund, strategy), temp_df in last_position_df.groupby(["Fund", "Strategy"]):
            temp_df.loc[:, "Last_Market_Value"] = temp_df["Vol"] * temp_df["PrevClose"]
            last_market_value = temp_df["Last_Market_Value"].sum()
            last_market_list.append([fund, strategy, last_market_value])
        last_market_df = pd.DataFrame(last_market_list, columns=["Fund", "Strategy", "Last_Market_Value"])

        market_list = []
        position_df = pd.merge(position_df, self.__instrument_df, how="left", on=["Symbol"])
        for (fund, strategy), temp_df in position_df.groupby(["Fund", "Strategy"]):
            temp_df.loc[:, "Market_Value"] = temp_df["Vol"] * temp_df["NominalPrice"]
            market_value = temp_df["Market_Value"].sum()
            pnl = temp_df["Pnl"].sum()
            market_list.append([fund, strategy, market_value, pnl])
        market_df = pd.DataFrame(market_list, columns=["Fund", "Strategy", "Market_Value", "Pnl"])
        market_df = pd.merge(last_market_df, market_df, how="left", on=["Fund", "Strategy"])
        market_df["Market_Value_Avg"] = (market_df["Last_Market_Value"] + market_df["Market_Value"]) / 2

        total_alpha = (market_df["Pnl"].sum() / market_df["Market_Value_Avg"].sum()) - self.__sh000905_chg
        total_market_list = []
        for fund, temp_df in market_df.groupby("Fund"):
            total_market_list.append([fund, "Total", temp_df["Pnl"].sum(), temp_df["Market_Value_Avg"].sum()])

        for strategy, temp_df in market_df.groupby("Strategy"):
            total_market_list.append(["Total", strategy, temp_df["Pnl"].sum(), temp_df["Market_Value_Avg"].sum()])
        total_market_df = pd.DataFrame(total_market_list, columns=["Fund", "Strategy", "Pnl", "Market_Value_Avg"])
        total_market_df = pd.concat(
            [market_df[["Fund", "Strategy", "Pnl", "Market_Value_Avg"]], total_market_df], sort=True
        )

        total_market_df["Alpha"] = (total_market_df["Pnl"] / total_market_df["Market_Value_Avg"]) - self.__sh000905_chg

        alpha_report_df = total_market_df.pivot_table(
            index="Strategy", columns="Fund", values="Alpha", aggfunc=np.sum
        ).fillna(0)
        total_col = alpha_report_df.pop("Total")
        alpha_report_df.insert(len(alpha_report_df.columns), total_col.name, total_col)
        alpha_report_df.iloc[-1, -1] = total_alpha
        alpha_report_df = pd.concat([alpha_report_df, self.__basis_df], sort=True).fillna(0)

        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        for (strategy, temp_value) in alpha_report_df.to_dict("index").items():
            for (fund, chg_value) in temp_value.items():
                daily_stock_alpha = DailyStockAlpha()
                daily_stock_alpha.date = self.__date_str
                daily_stock_alpha.account = fund
                daily_stock_alpha.strategy = strategy
                daily_stock_alpha.alpha = float(chg_value)
                session_jobs.merge(daily_stock_alpha)
        session_jobs.commit()


class StkIntraDayReportJob:
    def __init__(self, date_str=None):
        if not date_str:
            date_str = date_utils.get_today_str("%Y-%m-%d")
        self.__date_str = date_str
        self.__instrument_df = None
        self.__position_df = pd.DataFrame(columns=["Fund", "Symbol", "Account_Qty"])
        self.__basket_df = pd.DataFrame(columns=["Fund", "Symbol", "BasketQty"])
        self.__trade_df = pd.DataFrame(columns=["Fund", "Symbol", "Pnl", "TradeMoney", "TradeTimes"])
        self.__basket_df = None
        self.__log_info_dict = {}
        self.__error_list = []

        self.__account_charge_df = pd.DataFrame(columns=["Fund", "Charge", "MinCommission"])
        self.__t0_parameter = []
        self.server_host = server_constant.get_server_model("host")
        self.session_history = self.server_host.get_db_session("history")

    def start_index(self, server_list):
        self.__query_charge_info()
        self.__query_t0_parameter()
        self.__query_prev_close()
        self.__filter_trade_log(server_list)

        for server_name in server_list:
            self.__query_usable_position(server_name)
            self.__query_basket_df(server_name)
            self.__query_order(server_name)
        self.__build_intraday_report()

    def __query_charge_info(self):
        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        list_fund_account = []
        for x in session_jobs.query(FundAccountInfo).filter(FundAccountInfo.enable == 1):
            if x.type not in (FUND_ACCOUNT_TYPE_ENUMS.Stock.value, FUND_ACCOUNT_TYPE_ENUMS.Credit.value):
                continue
            if not x.service_charge or "commission_rate" not in x.service_charge:
                continue
            dict_charge = json.loads(x.service_charge)
            commission_rate = float(dict_charge["commission_rate"].replace("%", "")) / 100
            transfer_fee = float(dict_charge["transfer_fee"].replace("%", "")) / 100
            min_commission = float(dict_charge["min_commission"])
            charge = commission_rate + transfer_fee
            list_fund_account.append([x.account_name, x.product_name, charge, min_commission])
        df_fund_account = pd.DataFrame(list_fund_account, columns=["Account", "Fund", "Charge", "MinCommission"])

        list_real_account = []
        for (_, account_list) in list(const.EOD_CONFIG_DICT["server_account_dict"].items()):
            for real_account in account_list:
                list_real_account.append(real_account.accountname)
        df_fund_account = df_fund_account[df_fund_account["Account"].isin(list_real_account)]
        df_fund_account = df_fund_account[["Fund", "Charge", "MinCommission"]]
        df_fund_account = df_fund_account.set_index("Fund", drop=False)
        df_fund_account.index.name = None
        self.__account_charge_df = df_fund_account

    def __filter_trade_log(self, server_list):
        log_file_list = []
        for server_name in server_list:
            log_folder_path = const.EOD_CONFIG_DICT["log_backup_folder_template"] % server_name
            for log_file_name in os.listdir(log_folder_path):
                if "intraday_log_" in log_file_name and self.__date_str.replace("-", "") in log_file_name:
                    log_file_list.append("%s/%s" % (log_folder_path, log_file_name))

        result = dict(Symbol=[], Log_Time=[], Trade_Type=[], Side=[], Fund_Name=[])
        regex = re.compile(
            r"^\[.+?\]\s+\[.+?-(?P<Fund_Name>[^-]+?)-_(?P<Symbol>[\dA-Z]+)\]\s+\[.+?\]\s+.+?\s+(?P<Log_Time>"
            r"[\d\.:]+)(\s+(?P<Side>\w+)\s+(?P<Trade_Type>\w+)\s+@)\s+(?P<Pair>(?:\w+=[^\s]+[,\s]*)+$)"
        )
        regex_fund_name = re.compile(r"^.*?[:]*\[.+?\]\s+\[.+?-(?P<Fund_Name>[^-]+?)-_[\dA-Z]+\].+?")
        filter_list = ["Can", "not", "open", "exceed", "max"]

        filter_dict = {}
        for log_file_path in log_file_list:
            with open(log_file_path, "rb") as file_log:
                for line in file_log:
                    if self.__filter_line(filter_list, line):
                        match_result = regex_fund_name.match(line)
                        if match_result:
                            fund_name = regex_fund_name.match(line).groupdict()["Fund_Name"]
                            if fund_name in filter_dict:
                                filter_dict[fund_name] += 1
                            else:
                                filter_dict.setdefault(fund_name, 1)

                    if "Buy " in line or "Sell " in line:
                        match_result = regex.match(line)
                        if match_result:
                            group_dict = match_result.groupdict()
                            result["Symbol"].append(group_dict["Symbol"])
                            result["Log_Time"].append(group_dict["Log_Time"])
                            result["Trade_Type"].append(group_dict["Trade_Type"])
                            result["Side"].append(group_dict["Side"])
                            result["Fund_Name"].append(group_dict["Fund_Name"])
                            for pair in group_dict["Pair"].split(","):
                                field, value = pair.strip().split("=")
                                result.setdefault(field, []).append(value)

        df_trade = pd.DataFrame(result)
        for fund_name, temp_df in df_trade.groupby("Fund_Name"):
            open_num = len(temp_df[temp_df["Trade_Type"] == "Open"])
            limit_open_num = filter_dict[fund_name] if fund_name in filter_dict else 0
            self.__log_info_dict[fund_name] = (open_num, limit_open_num)

    @staticmethod
    def __filter_line(list_filter, line):
        for filter_str in list_filter:
            if filter_str not in line:
                return False
        return True

    def __query_t0_parameter(self):
        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        for x in session_jobs.query(FundParameterHistory).filter(FundParameterHistory.date == self.__date_str):
            if x.t0_channel is None or x.t0_channel == "":
                continue
            self.__t0_parameter.append(dict(Server=x.server_name, Fund=x.fund_name, Algo=x.t0_channel))

    def __query_prev_close(self):
        with InstrumentApiAdapter() as query_adapter:
            ticker_type_list = [INSTRUMENT_TYPE_ENUMS.CommonStock.value]
            common_ticker_list = query_adapter.get_ticker_list(ticker_type_list)
            close_dict = query_adapter.get_close_dict(self.__date_str, common_ticker_list)
            prev_close_dict = query_adapter.get_close_dict(self.__date_str, common_ticker_list)
            instrument_list = [[x, prev_close_dict[x], close_dict[x]] for x in common_ticker_list]
        self.__instrument_df = pd.DataFrame(instrument_list, columns=["Symbol", "Prev_Close", "Close"])
        self.__instrument_df = self.__instrument_df.set_index("Symbol", drop=False)
        self.__instrument_df.index.name = None

    def __query_order(self, server_name):
        server_model = server_constant.get_server_model(server_name)
        session_om = server_model.get_db_session("om")
        trade_list = []
        for x in session_om.query(Trade2History).filter(
            Trade2History.time.like("%" + self.__date_str + "%"),
            Trade2History.strategy_id.like("%StkIntraDayStrategy%"),
        ):
            fund = x.account.split("-")[2]
            symbol = x.symbol.split(" ")[0]
            trade_list.append([fund, symbol, x.qty, x.price])
        trade_df = pd.DataFrame(trade_list, columns=["Fund", "Symbol", "Qty", "Price"])
        trade_df = pd.merge(trade_df, self.__account_charge_df, how="left", on=["Fund"]).fillna(0.00015)

        trade_df.loc[:, "TradeMoney"] = trade_df["Qty"] * trade_df["Price"]
        trade_df.loc[trade_df["Qty"] > 0, "Fee"] = trade_df["TradeMoney"] * trade_df["Charge"]
        trade_df.loc[trade_df["Qty"] < 0, "Fee"] = abs(trade_df["TradeMoney"]) * (trade_df["Charge"] + 0.001)

        merge_trade_list = []
        for merge_key, sub_df in trade_df.groupby(["Fund", "Symbol"]):
            fund, symbol = merge_key
            pnl_exposure = 0
            fee_exposure = 0
            if sub_df["Qty"].sum() != 0:
                self.__error_list.append(
                    "[Error]Fund:%s,Symbol:%s Trade Qty Not Make Zero!Diff Qty:%s" % (fund, symbol, sub_df["Qty"].sum())
                )
                repair_qty = -sub_df["Qty"].sum()
                repair_price = self.__instrument_df.loc[symbol, "Close"]
                repair_trade_money = repair_qty * repair_price

                repair_charge = sub_df["Charge"].mean()
                repair_fee = (
                    repair_trade_money * repair_charge
                    if repair_qty > 0
                    else abs(repair_trade_money) * (repair_charge + 0.001)
                )
                sub_df = sub_df.append(
                    {
                        "Fund": fund,
                        "Symbol": symbol,
                        "Qty": repair_qty,
                        "Price": repair_price,
                        "TradeMoney": repair_trade_money,
                        "Charge": repair_charge,
                        "Fee": repair_fee,
                    },
                    ignore_index=True,
                )
                pnl_exposure = -sub_df["TradeMoney"].sum() - sub_df["Fee"].sum()
                fee_exposure = sub_df["Fee"].sum()
            fee = sub_df["Fee"].sum()
            pnl = -sub_df["TradeMoney"].sum() - sub_df["Fee"].sum()
            trade_money = sub_df[sub_df["Qty"] > 0]["TradeMoney"].sum()
            trade_times = len(sub_df[sub_df["Qty"] > 0])
            merge_trade_list.append([fund, symbol, pnl, pnl_exposure, fee, fee_exposure, trade_money, trade_times])
        merge_trade_df = pd.DataFrame(
            merge_trade_list,
            columns=["Fund", "Symbol", "Pnl", "ExposurePnl", "Fee", "ExposureFee", "TradeMoney", "TradeTimes"],
        )
        self.__trade_df = pd.concat([self.__trade_df, merge_trade_df], sort=True)

    def __query_basket_df(self, server_name):
        basket_df = pd.DataFrame(columns=["Fund", "Symbol", "BasketQty"])
        basket_folder = "%s/%s/%s_change" % (STOCK_SELECTION_FOLDER, server_name, self.__date_str.replace("-", ""))
        for file_name in os.listdir(basket_folder):
            if not file_name.endswith(".txt"):
                continue
            strategy_file_path = os.path.join(basket_folder, file_name)
            file_df = pd.read_csv(strategy_file_path, header=None, names=["Symbol", "BasketQty"], dtype={"Symbol": str})
            file_df = file_df[file_df["BasketQty"] < 0]
            file_df["Fund"] = file_name.split("@")[0].split("-")[2]
            basket_df = pd.concat([basket_df, file_df], sort=True)
        basket_df = basket_df.groupby(["Fund", "Symbol"]).sum().reset_index()
        self.__basket_df = pd.concat([self.__basket_df, basket_df], sort=True)

    def __query_usable_position(self, server_name):
        filter_date_str = date_utils.get_last_trading_day("%Y-%m-%d", self.__date_str)
        server_model = server_constant.get_server_model(server_name)
        session_portfolio = server_model.get_db_session("portfolio")

        usable_position_list = []
        for x in (
            session_portfolio.query(PfAccount.fund_name, PfPosition.symbol, PfPosition.yd_position_long)
            .join(PfPosition, PfAccount.id == PfPosition.id)
            .filter(PfPosition.date == filter_date_str)
        ):
            if not x[1].isdigit():
                continue
            fund = x[0].split("-")[2]
            usable_position_list.append([fund, x[1], x[2]])
        position_df = pd.DataFrame(usable_position_list, columns=["Fund", "Symbol", "FundQty"])
        position_df = position_df.groupby(["Fund", "Symbol"]).sum().reset_index()
        self.__position_df = pd.concat([self.__position_df, position_df], sort=True)

    def __calc_third_party_t0(self):
        list_df_t0 = []
        for t0_param in self.__t0_parameter:
            fund = t0_param["Fund"]
            server = t0_param["Server"]
            algo = t0_param["Algo"]
            multi_folder_path = "%s/%s/%s_%s" % (
                STOCK_SELECTION_FOLDER,
                server,
                self.__date_str.replace("-", ""),
                "multi",
            )
            if algo == T0_CHANNEL_ENUMS.KF_Local_T0.value:
                df_t0_calc = self.__cal_kafang_local_t0(server, fund)
                df_t0_calc["AlgoType"] = T0_CHANNEL_ENUMS.KF_Local_T0.value
                list_df_t0.append(df_t0_calc)

            elif algo in (T0_CHANNEL_ENUMS.Smart_KF_T0.value, T0_CHANNEL_ENUMS.Smart_ZC_T0.value):
                basket_order_columns = [
                    "BasketID",
                    "AlgoType",
                    "Status",
                    "BasketVol",
                    "BasketMoney",
                    "Symbol",
                    "OrderMoney",
                    "TradeMoney",
                    "OrdVol",
                    "TradeVol",
                    "StartTime",
                    "EndTime",
                    "AlgoPrice",
                    "MarketPrice",
                    "OrderID",
                    "Note",
                    "Direction",
                    "MarketSlippage",
                ]
                basket_folder_path = os.path.join(multi_folder_path, fund)
                basket_order_list = []
                for file_name in os.listdir(basket_folder_path):
                    if "母单列表" in file_name and file_name.endswith(".csv"):
                        sub_order_df = pd.read_csv(
                            "%s/%s" % (basket_folder_path, file_name),
                            encoding="utf-8",
                            names=basket_order_columns,
                            header=0,
                            dtype={"BasketID": str, "OrderID": str},
                        )
                        sub_order_df = sub_order_df[["BasketID", "AlgoType", "OrderID"]]
                        basket_order_list.append(sub_order_df)

                df_monitor_info = pd.concat(basket_order_list, ignore_index=True)
                if algo == T0_CHANNEL_ENUMS.Smart_ZC_T0.value:
                    df_monitor_info = df_monitor_info[df_monitor_info["AlgoType"] == T0_CHANNEL_ENUMS.Smart_ZC_T0.value]
                elif algo == T0_CHANNEL_ENUMS.Smart_KF_T0.value:
                    df_monitor_info = df_monitor_info[df_monitor_info["AlgoType"] == "KF-T0"]
                df_monitor_info = df_monitor_info[["BasketID", "AlgoType", "OrderID"]]

                order_file_name = ""

                for file_name in os.listdir(os.path.join(multi_folder_path, fund)):
                    if "_Trade%s.zip" % self.__date_str in file_name:
                        order_file_name = file_name
                order_file_path = os.path.join(multi_folder_path, fund, order_file_name)
                trade_columns = [
                    "AccountID",
                    "BasketID",
                    "Side",
                    "Symbol",
                    "Name",
                    "Exchange",
                    "Price",
                    "Qty",
                    "TradeMoney",
                    "TradeTime",
                    "XtpID",
                    "Channel",
                    "RowID",
                ]
                df_trade = pd.read_csv(
                    order_file_path,
                    encoding="utf-8",
                    names=trade_columns,
                    header=0,
                    dtype={"BasketID": str, "Symbol": str},
                )
                df_trade = df_trade.merge(df_monitor_info, on=["BasketID"], how="right").dropna()
                df_trade = df_trade[["Side", "Symbol", "Price", "Qty", "TradeMoney"]]
                df_trade["Symbol"] = df_trade["Symbol"].str.strip()
                trade_num = len(df_trade)
                df_trade["Side"] = df_trade["Side"].apply(lambda x: -1 if x == "卖" else 1)
                df_trade["Qty"] = df_trade["Qty"] * df_trade["Side"]
                df_trade["TradeMoney"] = df_trade["TradeMoney"] * df_trade["Side"]
                df_sum = df_trade.pivot_table(index=["Symbol"], values=["Qty"], aggfunc=[np.sum])
                df_sum.columns = ["SumQty"]
                df_sum = df_sum.reset_index()
                df_trade = df_trade.merge(df_sum, on=["Symbol"], how="left")
                df_trade["Charge"] = self.__account_charge_df.at[fund, "Charge"]

                df_exposure = df_trade[df_trade["SumQty"] != 0]
                df_repair = pd.DataFrame(columns=["Fund", "Symbol", "Qty", "Price", "TradeMoney", "Charge", "SumQty"])
                for _, row in df_sum[df_sum["SumQty"] != 0].iterrows():
                    repair_qty = abs(row["SumQty"])
                    side = 1 if row["SumQty"] < 0 else -1
                    symbol = row["Symbol"]
                    repair_price = self.__instrument_df.loc[symbol, "Close"]
                    trade_money = side * repair_qty * repair_price
                    df_repair = df_repair.append(
                        {
                            "Fund": fund,
                            "Symbol": symbol,
                            "Qty": repair_qty,
                            "Price": repair_price,
                            "TradeMoney": trade_money,
                            "Charge": self.__account_charge_df.at[fund, "Charge"],
                            "SumQty": repair_qty,
                        },
                        ignore_index=True,
                    )
                if not df_repair.empty:
                    df_exposure = pd.concat([df_exposure, df_repair], ignore_index=True, sort=True)
                    df_trade = pd.concat([df_trade, df_repair], ignore_index=True, sort=True)

                df_trade.loc[df_trade["TradeMoney"] > 0, "Fee"] = df_trade["TradeMoney"] * df_trade["Charge"]
                df_trade.loc[df_trade["TradeMoney"] < 0, "Fee"] = df_trade["TradeMoney"].abs() * (
                    df_trade["Charge"] + 0.001
                )
                df_exposure.loc[df_exposure["TradeMoney"] > 0, "Fee"] = (
                    df_exposure["TradeMoney"] * df_exposure["Charge"]
                )
                df_exposure.loc[df_exposure["TradeMoney"] < 0, "Fee"] = df_exposure["TradeMoney"].abs() * (
                    df_exposure["Charge"] + 0.001
                )

                pnl = -df_trade["TradeMoney"].sum()
                fee = df_trade["Fee"].sum()
                pnl = pnl - fee

                pnl_exposure = -df_exposure["TradeMoney"].sum()
                fee_exposure = df_exposure["Fee"].sum()
                pnl_exposure = pnl_exposure - fee_exposure
                cum_value = df_trade["TradeMoney"].abs().sum()

                t0_order_file_path = os.path.join(multi_folder_path, fund, "order_%s.csv" % algo)
                list_order = []
                with open(t0_order_file_path) as file_order:
                    list_order_line = [line.replace("\n", "").split(",") for line in file_order.readlines()]
                    for order_line in list_order_line:
                        if algo == T0_CHANNEL_ENUMS.Smart_ZC_T0.value:
                            list_order.append([str(order_line[1]), order_line[4]])
                        elif algo == T0_CHANNEL_ENUMS.Smart_KF_T0.value:
                            list_order.append([str(order_line[2]), order_line[4]])

                df_order = pd.DataFrame(list_order, columns=["Symbol", "OrderVol"])
                df_order["Symbol"] = df_order["Symbol"].apply(lambda x: x.split(".")[0])
                df_order = df_order.merge(self.__instrument_df, on="Symbol")
                df_order = df_order[df_order["OrderVol"] != ""]
                df_order["OrderVol"] = df_order["OrderVol"].astype(int)
                df_order["OrderValue"] = df_order[["OrderVol", "Prev_Close"]].apply(lambda x: x[0] * x[1], axis=1)
                order_value = df_order["OrderValue"].sum() * 2
                len_order_symbol = len(set(df_order["Symbol"].values.tolist()))
                len_trade_symbol = len(set(df_trade["Symbol"].values.tolist()))
                pnl_trade_per = "%.2f%%" % (pnl * 100.0 / cum_value)
                pnl_per = "%.2f%%" % (pnl * 100.0 / order_value)
                symbol_use_per = "%.2f%%" % (len_trade_symbol * 100.0 / len_order_symbol)
                user_per = "%.2f%%" % (cum_value * 100.0 / order_value)
                df_t0_calc = pd.DataFrame(
                    [
                        [
                            fund,
                            "{:,.0f}".format(pnl),
                            "{:,.0f}".format(pnl - pnl_exposure),
                            "{:,.0f}".format(pnl_exposure),
                            "{:,.0f}".format(fee),
                            "{:,.0f}".format(fee_exposure),
                            "{:,.0f}".format(cum_value),
                            "{:,.0f}".format(order_value),
                            "{:,.0f}".format(trade_num),
                            0,
                            "{:,.0f}".format(len_order_symbol),
                            "{:,.0f}".format(len_trade_symbol),
                            algo,
                            pnl_trade_per,
                            pnl_per,
                            symbol_use_per,
                            user_per,
                            "0%",
                        ]
                    ],
                    columns=[
                        "Fund",
                        "Pnl",
                        "PnlEx",
                        "ExposurePnl",
                        "Fee",
                        "ExposureFee",
                        "CumValue",
                        "OrderValue",
                        "TradeNum",
                        "LimitOrderNum",
                        "UsableSymbolNum",
                        "UsedSymbolNum",
                        "AlgoType",
                        "PnlTradePer",
                        "PnlPer",
                        "SymbolUsePer",
                        "UserPer",
                        "TradeOrderPer",
                    ],
                )
                list_df_t0.append(df_t0_calc)
            elif algo == T0_CHANNEL_ENUMS.XunTou_CS_T0.value:
                df_t0_calc = self.__cal_xuntou_t0(server, fund)
                df_t0_calc["AlgoType"] = T0_CHANNEL_ENUMS.XunTou_CS_T0.value
                list_df_t0.append(df_t0_calc)
        df_t0 = pd.concat(list_df_t0, ignore_index=True, sort=True)
        df_t0 = df_t0.rename(
            columns={
                "Fund": "账户",
                "AlgoType": "日内算法",
                "Pnl": "盈亏",
                "PnlEx": "做平部分盈亏",
                "ExposurePnl": "敞口产生盈亏",
                "Fee": "交易费用",
                "ExposureFee": "敞口交易费用",
                "CumValue": "交易金额(双边)",
                "TradeNum": "交易笔数",
                "OrderValue": "可交易底仓金额(双边)",
                "UserPer": "底仓使用率",
                "PnlTradePer": "交易收益率",
                "PnlPer": "底仓收益率",
                "UsableSymbolNum": "可交易底仓股票数",
                "UsedSymbolNum": "交易使用股票数",
                "SymbolUsePer": "可交易底仓股票使用率",
                "LimitOrderNum": "委托单数",
                "TradeOrderPer": "成交委托比",
            }
        )

        return df_t0

    def __cal_kafang_local_t0(self, server, fund):
        fund_folder_path = f"{STOCK_SELECTION_FOLDER}/{server}/{self.__date_str.replace('-', '')}_multi/{fund}"
        order_algo_path = os.path.join(fund_folder_path, "orderAlgo.csv")
        df_order_algo = pd.read_csv(order_algo_path, dtype={"clOrdId": str})
        df_order_algo["clOrdId"] = df_order_algo["clOrdId"].str.strip()
        df_order_algo = df_order_algo.rename(columns={"symbol": "Symbol", "clientName": "Fund", "clOrdId": "ParentId"})
        df_order_algo = df_order_algo[(df_order_algo["Fund"] == fund) & (df_order_algo["ordType"] == "T0")]
        df_order_algo["Symbol"] = df_order_algo["Symbol"].apply(lambda x: x.split(".")[0])
        df_order_algo = df_order_algo.merge(self.__instrument_df, on="Symbol", how="left")
        df_order_algo["OrderValue"] = df_order_algo["orderQty"] * df_order_algo["Prev_Close"]
        order_value = df_order_algo["OrderValue"].sum() * 2
        len_order_symbol = len(set(df_order_algo["Symbol"].values.tolist()))
        df_order_algo = df_order_algo[["Fund", "ParentId", "ordType"]]

        order_actual_path = os.path.join(fund_folder_path, "orderActual.csv")
        df_order_actual = pd.read_csv(order_actual_path, dtype={"quoteId": str})
        df_order_actual["quoteId"] = df_order_actual["quoteId"].str.strip()
        df_order_actual = df_order_actual.rename(columns={"symbol": "Symbol", "quoteId": "ParentId"})
        df_order_actual = df_order_actual.merge(df_order_algo, on="ParentId", how="right")
        df_order_actual = df_order_actual[-df_order_actual["Symbol"].isna()]
        limit_order_num = len(df_order_actual)
        # 'Canceled' 状态的单也存在部分成交
        trade_num = len(df_order_actual[df_order_actual["ordStatus"] != "Canceled"])
        df_order_actual.loc[:, "Symbol"] = df_order_actual["Symbol"].apply(lambda x: x.split(".")[0])
        df_order_actual["side"] = df_order_actual["side"].apply(lambda x: -1 if x == "S" else 1)
        df_order_actual["cumQty"] = df_order_actual["cumQty"] * df_order_actual["side"]
        df_order_actual["TradeMoney"] = df_order_actual["cumQty"] * df_order_actual["avgPx"]
        df_order_actual = df_order_actual[["Fund", "Symbol", "cumQty", "avgPx", "TradeMoney"]]

        df_sum = df_order_actual.pivot_table(index=["Symbol"], values=["cumQty"], aggfunc=[np.sum])
        df_sum.columns = ["SumQty"]
        df_sum = df_sum.reset_index()
        df_order_actual = df_order_actual.merge(df_sum, on=["Symbol"], how="left")
        df_order_actual["Charge"] = self.__account_charge_df.at[fund, "Charge"]

        df_exposure = df_order_actual[df_order_actual["SumQty"] != 0]

        df_repair = pd.DataFrame(columns=["Fund", "Symbol", "cumQty", "avgPx", "TradeMoney", "Charge", "SumQty"])
        for _, row in df_sum[df_sum["SumQty"] != 0].iterrows():
            repair_qty = abs(row["SumQty"])
            side = 1 if row["SumQty"] < 0 else -1
            symbol = row["Symbol"]
            repair_price = self.__instrument_df.loc[symbol, "Close"]
            trade_money = side * repair_qty * repair_price
            df_repair = df_repair.append(
                {
                    "Fund": fund,
                    "Symbol": symbol,
                    "cumQty": repair_qty,
                    "avgPx": repair_price,
                    "TradeMoney": trade_money,
                    "Charge": self.__account_charge_df.at[fund, "Charge"],
                    "SumQty": repair_qty,
                },
                ignore_index=True,
            )
        if not df_repair.empty:
            df_exposure = pd.concat([df_exposure, df_repair], ignore_index=True, sort=True)
            df_order_actual = pd.concat([df_order_actual, df_repair], ignore_index=True, sort=True)

        df_order_actual.loc[df_order_actual["TradeMoney"] > 0, "Fee"] = (
            df_order_actual["TradeMoney"] * df_order_actual["Charge"]
        )
        df_order_actual.loc[df_order_actual["TradeMoney"] < 0, "Fee"] = df_order_actual["TradeMoney"].abs() * (
            df_order_actual["Charge"] + 0.001
        )
        df_exposure.loc[df_exposure["TradeMoney"] > 0, "Fee"] = df_exposure["TradeMoney"] * df_exposure["Charge"]
        df_exposure.loc[df_exposure["TradeMoney"] < 0, "Fee"] = df_exposure["TradeMoney"].abs() * (
            df_exposure["Charge"] + 0.001
        )

        pnl = -df_order_actual["TradeMoney"].sum()
        fee = df_order_actual["Fee"].sum()
        pnl = pnl - fee

        pnl_exposure = -df_exposure["TradeMoney"].sum()
        fee_exposure = df_exposure["Fee"].sum()
        pnl_exposure = int(pnl_exposure - fee_exposure)

        cum_value = df_order_actual["TradeMoney"].abs().sum()
        len_trade_symbol = len(set(df_order_actual["Symbol"].values.tolist()))
        pnl_trade_per = "%.2f%%" % (pnl * 100.0 / cum_value)
        pnl_per = "%.2f%%" % (pnl * 100.0 / order_value)
        symbol_use_per = "%.2f%%" % (len_trade_symbol * 100.0 / len_order_symbol)
        user_per = "%.2f%%" % (cum_value * 100.0 / order_value)
        trade_order_per = "%.2f%%" % (trade_num * 100.0 / limit_order_num)
        return pd.DataFrame(
            [
                [
                    fund,
                    "{:,.0f}".format(pnl),
                    "{:,.0f}".format(pnl - pnl_exposure),
                    "{:,.0f}".format(pnl_exposure),
                    "{:,.0f}".format(fee),
                    "{:,.0f}".format(fee_exposure),
                    "{:,.0f}".format(cum_value),
                    "{:,.0f}".format(order_value),
                    "{:,.0f}".format(trade_num),
                    "{:,.0f}".format(limit_order_num),
                    "{:,.0f}".format(len_order_symbol),
                    "{:,.0f}".format(len_trade_symbol),
                    pnl_trade_per,
                    pnl_per,
                    symbol_use_per,
                    user_per,
                    trade_order_per,
                ]
            ],
            columns=[
                "Fund",
                "Pnl",
                "PnlEx",
                "ExposurePnl",
                "Fee",
                "ExposureFee",
                "CumValue",
                "OrderValue",
                "TradeNum",
                "LimitOrderNum",
                "UsableSymbolNum",
                "UsedSymbolNum",
                "PnlTradePer",
                "PnlPer",
                "SymbolUsePer",
                "UserPer",
                "TradeOrderPer",
            ],
        )

    def __cal_xuntou_t0(self, server, fund):
        server_model = server_constant.get_server_model(server)
        session_portfolio = server_model.get_db_session("portfolio")
        dict_account_fund = {}
        for real_account_db in session_portfolio.query(RealAccount).filter(RealAccount.enable == 1):
            dict_account_fund[str(real_account_db.accountname)] = real_account_db.fund_name

        fund_folder_path = f"{STOCK_SELECTION_FOLDER}/{server}/{self.__date_str.replace('-', '')}_multi/{fund}"
        trade_path = os.path.join(fund_folder_path, "T0.csv")
        columns = [
            "Account",
            "Time",
            "Symbol",
            "Name",
            "Direction",
            "ExQty",
            "ExPrice",
            "TradeMoney",
            "TradeId",
            "ContractId",
            "InvestmentNote",
            "AccountNote",
            "OrderId",
        ]
        df_trade = pd.read_csv(trade_path, header=0, names=columns, dtype={"Symbol": str}, encoding="gbk")
        df_trade["Direction"] = df_trade["Direction"].apply(lambda x: -1 if "卖出" in x else 1)
        df_trade["ExQty"] = df_trade["ExQty"] * df_trade["Direction"]
        df_trade["TradeMoney"] = df_trade["TradeMoney"] * df_trade["Direction"]
        df_trade["Fund"] = df_trade["Account"].apply(lambda x: dict_account_fund[str(x)])
        df_trade = df_trade[["Fund", "Symbol", "ExQty", "ExPrice", "TradeMoney", "InvestmentNote"]]
        df_trade = df_trade[df_trade["Fund"] == fund]
        df_trade = df_trade[~df_trade["InvestmentNote"].isnull()]
        trade_num = len(df_trade)

        df_sum = df_trade.pivot_table(index=["Symbol"], values=["ExQty"], aggfunc=[np.sum])
        df_sum.columns = ["SumQty"]
        df_sum = df_sum.reset_index()
        df_trade = df_trade.merge(df_sum, on=["Symbol"], how="left")
        df_trade["Charge"] = self.__account_charge_df.at[fund, "Charge"]

        df_exposure = df_trade[df_trade["SumQty"] != 0]
        df_repair = pd.DataFrame(columns=["Fund", "Symbol", "ExQty", "ExPrice", "TradeMoney", "Charge", "SumQty"])
        for _, row in df_sum[df_sum["SumQty"] != 0].iterrows():
            repair_qty = abs(row["SumQty"])
            side = 1 if row["SumQty"] < 0 else -1
            symbol = row["Symbol"]
            repair_price = self.__instrument_df.loc[symbol, "Close"]
            trade_money = side * repair_qty * repair_price
            df_repair = df_repair.append(
                {
                    "Fund": fund,
                    "Symbol": symbol,
                    "ExQty": repair_qty,
                    "ExPrice": repair_price,
                    "TradeMoney": trade_money,
                    "Charge": self.__account_charge_df.at[fund, "Charge"],
                    "SumQty": repair_qty,
                },
                ignore_index=True,
            )
        if not df_repair.empty:
            df_exposure = pd.concat([df_exposure, df_repair], ignore_index=True, sort=True)
            df_trade = pd.concat([df_trade, df_repair], ignore_index=True, sort=True)

        df_trade.loc[df_trade["TradeMoney"] > 0, "Fee"] = df_trade["TradeMoney"] * df_trade["Charge"]
        df_trade.loc[df_trade["TradeMoney"] < 0, "Fee"] = df_trade["TradeMoney"].abs() * (df_trade["Charge"] + 0.001)
        df_exposure.loc[df_exposure["TradeMoney"] > 0, "Fee"] = df_exposure["TradeMoney"] * df_exposure["Charge"]
        df_exposure.loc[df_exposure["TradeMoney"] < 0, "Fee"] = df_exposure["TradeMoney"].abs() * (
            df_exposure["Charge"] + 0.001
        )

        pnl = -df_trade["TradeMoney"].sum()
        fee = df_trade["Fee"].sum()
        pnl = pnl - fee

        pnl_exposure = -df_exposure["TradeMoney"].sum()
        fee_exposure = df_exposure["Fee"].sum()
        pnl_exposure = int(pnl_exposure - fee_exposure)

        t0_order_file_path = os.path.join(fund_folder_path, "order_XunTou_CS_T0.csv")
        list_order = []
        with open(t0_order_file_path) as file_order:
            list_order_line = [line.replace("\n", "").split(",") for line in file_order.readlines()]
            for order_line in list_order_line:
                list_order.append([str(order_line[0].split(".")[0]), order_line[1]])

        df_order = pd.DataFrame(list_order, columns=["Symbol", "OrderVol"])
        df_order["Symbol"] = df_order["Symbol"].apply(lambda x: x.split(".")[0])
        df_order = df_order.merge(self.__instrument_df, on="Symbol")
        df_order = df_order[df_order["OrderVol"] != ""]
        df_order["OrderVol"] = df_order["OrderVol"].astype(int)
        df_order["OrderValue"] = df_order[["OrderVol", "Prev_Close"]].apply(lambda x: x[0] * x[1], axis=1)

        order_value = df_order["OrderValue"].sum() * 2
        len_order_symbol = len(set(df_order["Symbol"].values.tolist()))
        len_trade_symbol = len(set(df_trade["Symbol"].values.tolist()))
        cum_value = df_trade["TradeMoney"].abs().sum()
        pnl_trade_per = "%.2f%%" % (pnl * 100.0 / cum_value)
        pnl_per = "%.2f%%" % (pnl * 100.0 / order_value)
        symbol_use_per = "%.2f%%" % (len_trade_symbol * 100.0 / len_order_symbol)
        user_per = "%.2f%%" % (cum_value * 100.0 / order_value)
        # trade_order_per = "%.2f%%" % (trade_num * 100.0 / limit_order_num)

        return pd.DataFrame(
            [
                [
                    fund,
                    "{:,.0f}".format(pnl),
                    "{:,.0f}".format(pnl - pnl_exposure),
                    "{:,.0f}".format(pnl_exposure),
                    "{:,.0f}".format(fee),
                    "{:,.0f}".format(fee_exposure),
                    "{:,.0f}".format(cum_value),
                    "{:,.0f}".format(order_value),
                    "{:,.0f}".format(trade_num),
                    "0",
                    "{:,.0f}".format(len_order_symbol),
                    "{:,.0f}".format(len_trade_symbol),
                    pnl_trade_per,
                    pnl_per,
                    symbol_use_per,
                    user_per,
                    "0%",
                ]
            ],
            columns=[
                "Fund",
                "Pnl",
                "PnlEx",
                "ExposurePnl",
                "Fee",
                "ExposureFee",
                "CumValue",
                "OrderValue",
                "TradeNum",
                "LimitOrderNum",
                "UsableSymbolNum",
                "UsedSymbolNum",
                "PnlTradePer",
                "PnlPer",
                "SymbolUsePer",
                "UserPer",
                "TradeOrderPer",
            ],
        )

    def build_report_period(self, start_date, end_date):
        list_t0 = []
        for x in self.session_history.query(StockIntraday).filter(StockIntraday.date.between(start_date, end_date)):
            list_t0.append(
                [
                    x.fund,
                    x.algo,
                    x.pnl,
                    x.pnl_exposure,
                    x.fee,
                    x.fee_exposure,
                    x.amt_two_side,
                    x.trade_num,
                    x.order_num,
                    x.usable_value_two_side,
                    x.used_stock_num,
                    x.usable_stock_num,
                ]
            )
        df_t0 = pd.DataFrame(
            list_t0,
            columns=[
                "Fund",
                "Algo",
                "Pnl",
                "PnlExposure",
                "Fee",
                "ExposureFee",
                "AmtTwoSide",
                "TradeNum",
                "OrderNum",
                "UsableValueTwoSide",
                "UsedStockNum",
                "UsableStockNum",
            ],
        )
        df_t0 = df_t0.groupby(["Fund", "Algo"]).sum().reset_index()
        series_sum = df_t0.sum()
        series_sum["Fund"] = "/"
        series_sum["Algo"] = "z_Total"
        df_t0 = df_t0.append(series_sum, ignore_index=True)

        asset_value_sql = "select a.product_name, a.net_asset_value, a.date_str from asset_value_info a,\
    (select product_name, max(date_str) as date_str from asset_value_info where is_reckon=0 group by product_name) b \
    where a.product_name = b.product_name and a.date_str = b.date_str and a.is_reckon=0"
        session_jobs = self.server_host.get_db_session("jobs")
        query_result = [(x[0], float(x[1])) for x in session_jobs.execute(asset_value_sql)]
        dict_asset_value = dict(query_result)
        df_t0["NetAssetValue"] = df_t0["Fund"].apply(
            lambda x: dict_asset_value[x] if x in dict_asset_value.keys() else 0
        )

        df_t0["PerUseValue"] = df_t0[["AmtTwoSide", "UsableValueTwoSide"]].apply(
            lambda x: "%.4f%%" % (x[0] * 100 / x[1] if x[1] != 0 else 0), axis=1
        )
        df_t0["PerTradeOrder"] = df_t0[["TradeNum", "OrderNum"]].apply(
            lambda x: "%.4f%%" % (x[0] * 100 / x[1] if x[1] != 0 else 0), axis=1
        )
        df_t0["PerPnlTrade"] = df_t0[["Pnl", "AmtTwoSide"]].apply(
            lambda x: "%.4f%%" % (x[0] * 100 / x[1] if x[1] != 0 else 0), axis=1
        )
        df_t0["PerPnlUsable"] = df_t0[["Pnl", "UsableValueTwoSide"]].apply(
            lambda x: "%.4f%%" % (x[0] * 100 / x[1] if x[1] != 0 else 0), axis=1
        )
        df_t0["PerPnlAsset"] = df_t0[["Pnl", "NetAssetValue"]].apply(
            lambda x: "%.4f%%" % (x[0] * 100 / x[1] if x[1] != 0 else 0), axis=1
        )
        df_t0 = df_t0.rename(
            columns={
                "Algo": "日内算法",
                "Pnl": "盈亏",
                "AmtTwoSide": "交易金额(双边)",
                "TradeNum": "交易笔数",
                "OrderNum": "委托单数",
                "PerTradeOrder": "成交委托比",
                "UsableValueTwoSide": "可交易底仓金额(双边)",
                "PerUseValue": "底仓使用率",
                "PerPnlTrade": "交易收益率",
                "PerPnlUsable": "底仓收益率",
                "PerPnlAsset": "产品收益率",
            }
        )
        df_t0 = df_t0[
            [
                "Fund",
                "日内算法",
                "盈亏",
                "交易金额(双边)",
                "交易笔数",
                "委托单数",
                "成交委托比",
                "可交易底仓金额(双边)",
                "底仓使用率",
                "交易收益率",
                "底仓收益率",
                "产品收益率",
            ]
        ]
        return df_t0

    def __build_intraday_report(self):
        df_t0 = self.__calc_third_party_t0()

        merge_df = pd.merge(self.__position_df, self.__basket_df, how="left", on=["Fund", "Symbol"]).fillna(0)
        merge_df = pd.merge(merge_df, self.__trade_df, how="left", on=["Fund", "Symbol"]).fillna(0)
        merge_df = pd.merge(merge_df, self.__instrument_df, how="left", on=["Symbol"]).fillna(0)
        merge_df.loc[:, "UsableMoney"] = merge_df["FundQty"] * merge_df["Prev_Close"]

        list_third_t0_fund = [x["Fund"] for x in self.__t0_parameter]
        report_list = []
        for fund, sub_df in merge_df.groupby(["Fund"]):
            if fund in list_third_t0_fund:
                continue
            if fund in self.__log_info_dict:
                entrust_orders, unopen_entrust_orders = self.__log_info_dict[fund]
                unopen_entrust_per = "%.2f%%" % (unopen_entrust_orders * 100.0 / entrust_orders)
            else:
                entrust_orders, unopen_entrust_orders = 0, 0
                unopen_entrust_per = "0%"
            pnl = int(sub_df["Pnl"].sum())
            pnl_exposure = int(sub_df["ExposurePnl"].sum())
            fee = int(sub_df["Fee"].sum())
            fee_exposure = int(sub_df["ExposureFee"].sum())
            trade_money = int(sub_df["TradeMoney"].sum()) * 2
            trade_times = sub_df["TradeTimes"].sum()
            usable_money = int(sub_df["UsableMoney"].sum())

            usable_per = "%.2f%%" % (trade_money * 100.0 / usable_money) if usable_money > 0 else "0%"
            pnl_per = "%.4f%%" % (pnl * 100.0 / trade_money) if trade_money > 0 else "0%"
            pnl_per2 = "%.4f%%" % (pnl * 100.0 / usable_money) if usable_money > 0 else "0%"

            usable_symbol_size = len(sub_df[sub_df["Account_Qty"] + sub_df["BasketQty"] > 0])
            symbol_size = len(sub_df)
            trade_symbol_size = len(sub_df[sub_df["TradeTimes"] > 0])

            usable_symbol_per = (
                "%.f%%" % (trade_symbol_size * 100 / usable_symbol_size) if usable_symbol_size > 0 else "0%"
            )
            # symbol_per = '%.f%%' % (trade_symbol_size * 100 / symbol_size)
            report_list.append(
                [
                    fund,
                    T0_CHANNEL_ENUMS.TradePlat_T0.value,
                    "{:,.0f}".format(pnl),
                    "{:,.0f}".format(pnl - pnl_exposure),
                    "{:,.0f}".format(pnl_exposure),
                    "{:,.0f}".format(fee),
                    "{:,.0f}".format(fee_exposure),
                    "{:,.0f}".format(trade_money),
                    trade_times,
                    entrust_orders,
                    "{:,.0f}".format(unopen_entrust_orders),
                    unopen_entrust_per,
                    "{:,.0f}".format(usable_money),
                    usable_per,
                    pnl_per,
                    pnl_per2,
                    usable_symbol_size,
                    "{:,.0f}".format(symbol_size),
                    trade_symbol_size,
                    usable_symbol_per,
                ]
            )

        report_df = pd.DataFrame(
            report_list,
            columns=[
                "账户",
                "日内算法",
                "盈亏",
                "做平部分盈亏",
                "敞口产生盈亏",
                "交易费用",
                "敞口交易费用",
                "交易金额(双边)",
                "交易笔数",
                "委托单数",
                "限制开仓委托单数",
                "成交委托比",
                "可交易底仓金额(双边)",
                "底仓使用率",
                "交易收益率",
                "底仓收益率",
                "可交易底仓股票数",
                "总仓位股票数",
                "交易使用股票数",
                "可交易底仓股票使用率",
            ],
        )
        report_df = pd.concat([report_df, df_t0], ignore_index=True, sort=True).fillna("0")
        sum_pnl = report_df["盈亏"].astype(str).str.replace(",", "").astype(int).sum()
        sum_pnl_exposure = report_df["敞口产生盈亏"].astype(str).str.replace(",", "").astype(int).sum()
        sum_fee = report_df["交易费用"].astype(str).str.replace(",", "").astype(int).sum()
        sum_fee_exposure = report_df["敞口交易费用"].astype(str).str.replace(",", "").astype(int).sum()
        sum_trade_money = report_df["交易金额(双边)"].astype(str).str.replace(",", "").astype(int).sum()

        sum_trade_num = report_df["交易笔数"].astype(str).str.replace(",", "").astype(int).sum()
        sum_order_num = report_df["委托单数"].astype(str).str.replace(",", "").astype(int).sum()
        sum_limit_num = report_df["限制开仓委托单数"].astype(str).str.replace(",", "").astype(int).sum()
        sum_usable_money = report_df["可交易底仓金额(双边)"].astype(str).str.replace(",", "").astype(int).sum()
        sum_usable_num = report_df["可交易底仓股票数"].astype(str).str.replace(",", "").astype(int).sum()
        sum_total_num = report_df["总仓位股票数"].astype(str).str.replace(",", "").astype(int).sum()
        sum_used_num = report_df["交易使用股票数"].astype(str).str.replace(",", "").astype(int).sum()
        series_sum = pd.Series(
            [
                "/",
                "z_Total",
                "{:,.0f}".format(sum_pnl),
                "{:,.0f}".format(sum_pnl - sum_pnl_exposure),
                "{:,.0f}".format(sum_pnl_exposure),
                "{:,.0f}".format(sum_fee),
                "{:,.0f}".format(sum_fee_exposure),
                "{:,.0f}".format(sum_trade_money),
                "{:,.0f}".format(sum_trade_num),
                "{:,.0f}".format(sum_order_num),
                "{:,.0f}".format(sum_limit_num),
                "/",
                "{:,.0f}".format(sum_usable_money),
                "%.4f%%" % (sum_trade_money * 100.0 / sum_usable_money) if sum_usable_money > 0 else "0%",
                "%.4f%%" % (sum_pnl * 100.0 / sum_trade_money) if sum_trade_money > 0 else "0%",
                "%.4f%%" % (sum_pnl * 100.0 / sum_usable_money) if sum_usable_money > 0 else "0%",
                "{:,.0f}".format(sum_usable_num),
                "{:,.0f}".format(sum_total_num),
                "{:,.0f}".format(sum_used_num),
                "%.4f%%" % (sum_used_num * 100.0 / sum_usable_num) if sum_usable_num > 0 else "0%",
            ],
            index=[
                "账户",
                "日内算法",
                "盈亏",
                "做平部分盈亏",
                "敞口产生盈亏",
                "交易费用",
                "敞口交易费用",
                "交易金额(双边)",
                "交易笔数",
                "委托单数",
                "限制开仓委托单数",
                "成交委托比",
                "可交易底仓金额(双边)",
                "底仓使用率",
                "交易收益率",
                "底仓收益率",
                "可交易底仓股票数",
                "总仓位股票数",
                "交易使用股票数",
                "可交易底仓股票使用率",
            ],
        )
        report_df = report_df.append(series_sum, ignore_index=True)

        for _, row in report_df.iterrows():
            if row["日内算法"] == "z_Total":
                continue
            stock_intraday = StockIntraday()
            stock_intraday.fund = row["账户"]
            stock_intraday.date = self.__date_str
            stock_intraday.algo = row["日内算法"]
            stock_intraday.pnl = int(row["盈亏"].replace(",", ""))
            stock_intraday.pnl_exposure = int(row["敞口产生盈亏"].replace(",", ""))
            stock_intraday.fee = int(row["交易费用"].replace(",", ""))
            stock_intraday.fee_exposure = int(row["敞口交易费用"].replace(",", ""))
            stock_intraday.amt_two_side = int(row["交易金额(双边)"].replace(",", ""))
            stock_intraday.trade_num = (
                row["交易笔数"] if isinstance(row["交易笔数"], int) else int(row["交易笔数"].replace(",", ""))
            )
            stock_intraday.order_num = int(str(row["委托单数"]).replace(",", ""))
            stock_intraday.usable_value_two_side = int(row["可交易底仓金额(双边)"].replace(",", ""))
            stock_intraday.usable_stock_num = row["可交易底仓股票数"]
            stock_intraday.used_stock_num = row["交易使用股票数"]
            self.session_history.merge(stock_intraday)
        self.session_history.commit()

        report_df = report_df[
            [
                "账户",
                "日内算法",
                "盈亏",
                "做平部分盈亏",
                "敞口产生盈亏",
                "交易金额(双边)",
                "交易笔数",
                "委托单数",
                "限制开仓委托单数",
                "成交委托比",
                "可交易底仓金额(双边)",
                "底仓使用率",
                "交易收益率",
                "底仓收益率",
                "可交易底仓股票数",
                "总仓位股票数",
                "交易使用股票数",
                "可交易底仓股票使用率",
            ]
        ]
        report_list = email_utils.df_to_html_combined(report_df, ["日内算法", "账户"], td_align="right")
        html_content = "Date:%s<br>%s<br>%s" % (self.__date_str, "".join(report_list), "<br>".join(self.__error_list))
        email_utils.send_email_group_all("股票日内统计报告", html_content, "html")

    def check_third_part_statistic_miss(self):
        dict_risk = {}
        for risk_db in self.session_history.query(ServerRisk).filter(
            ServerRisk.date == self.__date_str, ServerRisk.strategy_name.like("%T0%")
        ):
            algo_type, _, fund_name, _ = risk_db.strategy_name.split("-")
            dict_risk[fund_name] = algo_type

        dict_intraday = {}
        for intraday_db in self.session_history.query(StockIntraday).filter(StockIntraday.date == self.__date_str):
            fund_name = intraday_db.fund
            algo = intraday_db.algo
            if algo == T0_CHANNEL_ENUMS.TradePlat_T0.value:
                continue
            dict_intraday[fund_name] = algo

        set_diff = set(dict_risk.items()) ^ set(dict_intraday.items())
        if set_diff:
            list_diff = [str(x) for x in set_diff]
            email_utils.send_email_group_all("股票日内统计报告有缺失", f"股票日内未统计：{','.join(list_diff)}", "html")


class VwapReportJob:
    def __init__(self, date_str=None):
        if not date_str:
            date_str = date_utils.get_today_str("%Y-%m-%d")
        self.__date_str = date_str
        self.__market_data_1min_folder = os.path.join(
            const.EOD_CONFIG_DICT["data_file_folder"],
            "wind",
            "stock",
            self.__date_str.replace("-", ""),
            "market_data_1min",
        )
        self.__list_account_third_algo = self.__query_algo_info()
        self.__dump_order_folder = "%s/%s" % (DAILY_FILES_FOLDER, "basket_order_pickle")
        self.__basket_df = None
        self.dict_basket_vwap = None
        self.ticker_vol_file_df = None
        self.dict_algo_config = None
        self.__ticker_avg_price_df = None

    def start_index(self, server_list):
        self.__query_algo_config()
        self.__query_basket_order_file(server_list)
        self.__query_avg_price_df()
        self.__build_vwap_report()

    def __query_algo_info(self):
        server_host = server_constant.get_server_model("host")
        session_jobs = server_host.get_db_session("jobs")
        fund_parameter_list = []
        for x in session_jobs.query(FundParameterHistory).filter(FundParameterHistory.date == self.__date_str):
            if x.vwap_channel is None or x.vwap_channel == "":
                continue
            if x.vwap_channel == "TradePlat":
                continue
            fund_parameter_list.append([x.server_name, x.fund_name, x.vwap_channel])
        df_strategy_account = pd.DataFrame(fund_parameter_list, columns=["server", "fund", "algo"])
        df_strategy_account = df_strategy_account.drop_duplicates()

        list_server = set(df_strategy_account["server"].values.tolist())
        list_real_account = []
        for server in list_server:
            server_model = server_constant.get_server_model(server)
            session_portfolio = server_model.get_db_session("portfolio")
            for real_account_db in session_portfolio.query(RealAccount).filter(
                RealAccount.allow_targets.contains("commonstock")
            ):
                fund = real_account_db.fund_name
                account = "%s-%s-%s-" % (real_account_db.accountname, real_account_db.accounttype, fund)

                list_real_account.append([server, fund, account])
        df_real_account = pd.DataFrame(list_real_account, columns=["server", "fund", "account"])
        df_algo_info = df_strategy_account.merge(df_real_account, on=["server", "fund"], how="left")
        del df_algo_info["fund"]
        return df_algo_info.to_dict(orient="record")

    def __query_basket_df(self, server_name):
        basket_df = pd.DataFrame(columns=["Account", "Symbol", "BasketQty"])
        basket_folder = "%s/%s/%s_change" % (STOCK_SELECTION_FOLDER, server_name, self.__date_str.replace("-", ""))
        for file_name in os.listdir(basket_folder):
            if not file_name.endswith(".txt"):
                continue
            strategy_file_path = os.path.join(basket_folder, file_name)
            file_df = pd.read_csv(strategy_file_path, header=None, names=["Symbol", "BasketQty"], dtype={"Symbol": str})
            file_df = file_df[file_df["BasketQty"] < 0]
            file_df["Account"] = file_name.split("@")[0].split("-")[2]
            basket_df = pd.concat([basket_df, file_df], sort=True)
        basket_df = basket_df.groupby(["Account", "Symbol"]).sum().reset_index()
        self.__basket_df = pd.concat([self.__basket_df, basket_df], sort=True)

    def dump_redis_basket_order(self):
        stock_order_list = []
        stock_strategy_list = STOCK_STRATEGY_LIST
        full_stock_strategy_list = ["%s.%s" % (x, x) for x in stock_strategy_list]
        order_ids = redis_query_manager.query_order_root_ids()
        for basket_order_view in redis_query_manager.query_order_view_list(order_ids):
            if basket_order_view is None:
                continue
            if basket_order_view["Type"] == "LimitOrder":
                continue
            if basket_order_view["Status"] == "Canceled":
                continue
            if basket_order_view["Strategy"] not in full_stock_strategy_list:
                continue

            # 'AlgoName': 8 Pair加减仓单  'AlgoName': 3  MarketBasket 调仓单
            if basket_order_view["AlgoName"] == 3:
                find_key = "%s|%s" % (basket_order_view["OrderID"], basket_order_view["Location"])
                sub_order_ids = redis_query_manager.query_sub_order_ids(find_key)
                server_name = basket_order_view["Server"]
                for sub_order_view in redis_query_manager.query_order_view_list(sub_order_ids):
                    symbol = sub_order_view["Symbol"].split(" ")[0]
                    direction = sub_order_view["Direction"]
                    order_qty = sub_order_view["OrdVol"]
                    ex_qty = sub_order_view["TradeVol"]
                    ex_price = sub_order_view["ExPrice"]
                    parent_ord_id = sub_order_view["ParentOrderID"]
                    cost = ex_price * ex_qty
                    transaction_time = sub_order_view["TransactionT"]
                    now_time = sub_order_view["TransactionT"]
                    strategy = sub_order_view["Strategy"]
                    create_time = sub_order_view["CreationT"]
                    account = sub_order_view["Account"]
                    stock_order_list.append(
                        [
                            server_name,
                            parent_ord_id,
                            symbol,
                            direction,
                            Decimal(order_qty),
                            Decimal(ex_qty),
                            Decimal(ex_price),
                            cost,
                            now_time,
                            transaction_time,
                            account,
                            strategy,
                            create_time,
                        ]
                    )
            else:
                continue

        stock_order_df = pd.DataFrame(
            stock_order_list,
            columns=[
                "Server_Name",
                "Parent_Order_ID",
                "Symbol",
                "Direction",
                "OrdVol",
                "ExQty",
                "Exprice",
                "Cost",
                "Now",
                "T",
                "Account",
                "Strategy",
                "CreateTime",
            ],
        )
        dump_path = os.path.join(self.__dump_order_folder, "stok_order_df_%s.pickle" % self.__date_str.replace("-", ""))
        stock_order_df.to_pickle(dump_path)

    def __load_basket_order(self):
        load_path = os.path.join(self.__dump_order_folder, "stok_order_df_%s.pickle" % self.__date_str.replace("-", ""))
        stock_order_df = pd.read_pickle(load_path)
        list_df = []
        for (server_name, parent_id), df_server_basket in stock_order_df.groupby(["Server_Name", "Parent_Order_ID"]):
            df_server_basket["Direction"] = df_server_basket["Direction"].apply(lambda x: 1 if x == "Buy" else -1)
            df_server_basket["OrdVol"] = df_server_basket["OrdVol"] * df_server_basket["Direction"]
            df_server_basket["Key"] = df_server_basket[["Symbol", "OrdVol"]].apply(
                lambda x: "%s|%s|" % (str(x[0]), str(x[1])), axis=1
            )

            df_server_basket = df_server_basket.sort_values("Symbol")
            key_vwap = server_name + "".join(df_server_basket["Key"].values.tolist())
            # 如果不能找到对应篮子单则过滤掉

            if key_vwap not in list(self.dict_basket_vwap.keys()):
                custom_log.log_info_task("parent id %s is not basket order" % parent_id)
                continue
            df_server_basket["Vwap_Type"] = self.dict_basket_vwap[key_vwap].split("_")[-1][:-3]
            df_server_basket["Server_Name"] = server_name
            list_df.append(df_server_basket)

        stock_order_df = pd.concat(list_df, ignore_index=True)
        return stock_order_df

    def __query_order_third_part(self):
        list_df = []
        for dict_account_third_algo in self.__list_account_third_algo:
            server = dict_account_third_algo["server"]
            account = dict_account_third_algo["account"]
            third_algo = dict_account_third_algo["algo"]
            if third_algo == VWAP_CHANNEL_ENUMS.KF_Smart.value:
                df_order = self.__query_third_part_kf(server, account)
            elif third_algo == VWAP_CHANNEL_ENUMS.ZiCheng.value:
                df_order = self.__query_third_part_zc(server, account)
            elif third_algo == VWAP_CHANNEL_ENUMS.KF_Local.value:
                df_order = self.__query_third_part_kflocal(server, account)
            elif third_algo == VWAP_CHANNEL_ENUMS.KF_CS.value:
                df_order = self.__query_third_part_kfcs(server, account)
            else:
                continue
            list_df.append(df_order)
        df_order_third_party = pd.concat(list_df, ignore_index=True, sort=True)
        df_order_third_party = df_order_third_party.sort_values("Symbol")
        return df_order_third_party

    def __query_third_part_zc(self, server, account):
        fund_name = account.split("-")[2]
        export_folder = "%s/%s/%s" % (STOCKSELECTION_FOLDER, server, "%s_multi" % self.__date_str.replace("-", ""))
        order_file_path = "%s/%s/%s" % (
            export_folder,
            fund_name,
            "%s_Trade%s.zip" % (account.split("-")[0], self.__date_str),
        )
        match_file_path = "%s/%s" % (export_folder, "order_map_%s.csv" % fund_name)

        df_match = pd.read_csv(match_file_path, names=["file_name", "vwap_order_id", "parent_id"], header=None)
        df_match.set_index("parent_id", inplace=True)
        dict_match = df_match["file_name"].to_dict()

        df_order_third = pd.read_csv(order_file_path, encoding="utf-8", dtype={"证券代码": str, "成交数量": str, "成交价格": str})

        df_order_third.rename(
            columns={
                "成交时间": "T",
                "证券代码": "Symbol",
                "方向": "Direction",
                "成交数量": "ExQty",
                "成交价格": "Exprice",
                "XTPID": "SysId",
                "母单编号": "ParentId",
            },
            inplace=True,
        )

        df_order_third = df_order_third[["ParentId", "T", "Symbol", "Direction", "ExQty", "Exprice", "SysId"]]
        df_order_third["Symbol"] = df_order_third["Symbol"].str.strip()
        df_order_third["ExQty"] = df_order_third["ExQty"].astype(float)
        df_order_third["Exprice"] = df_order_third["Exprice"].astype(float)
        df_order_third["Now"] = df_order_third["T"]
        df_order_third["Direction"] = df_order_third["Direction"].apply(lambda x: 1 if x == "买" else -1)
        df_order_third["Server_Name"] = VWAP_CHANNEL_ENUMS.ZiCheng.value
        df_order_third["Cost"] = df_order_third["ExQty"] * df_order_third["Exprice"]
        df_order_third["FileName"] = df_order_third["ParentId"].apply(
            lambda x: dict_match[str(x)] if str(x) in dict_match.keys() else ""
        )
        df_order_third = df_order_third[df_order_third["FileName"] != ""]
        df_order_third["Vwap_Type"] = df_order_third["FileName"].apply(lambda x: x.split("_")[-1].split(".")[0][:-4])
        df_order_third["OrdVol"] = 0
        return df_order_third

    def __query_third_part_kf(self, server, account):
        export_folder = "%s/%s/%s" % (STOCKSELECTION_FOLDER, server, "%s_multi" % self.__date_str.replace("-", ""))
        fund_name = account.split("-")[2]
        match_path = "%s/%s" % (export_folder, "order_map_%s.csv" % fund_name)
        df_id_file_match = pd.read_csv(match_path)
        df_id_file_match.rename(columns={"BasketID": "ParentId"}, inplace=True)
        df_id_file_match["FileName"] = df_id_file_match["FileName"].astype(str)
        df_id_file_match = df_id_file_match[df_id_file_match["FileName"] != "nan"]

        order_file_path = "%s/%s/%s" % (
            export_folder,
            fund_name,
            "%s_Trade%s.zip" % (account.split("-")[0], self.__date_str),
        )
        df_order_third = pd.read_csv(order_file_path, encoding="utf-8", dtype={"证券代码": str, "成交数量": str, "成交价格": str})
        df_order_third.rename(
            columns={
                "成交时间": "T",
                "证券代码": "Symbol",
                "方向": "Direction",
                "成交数量": "ExQty",
                "成交价格": "Exprice",
                "XTPID": "SysId",
                "母单编号": "ParentId",
            },
            inplace=True,
        )

        df_order_third = df_order_third.merge(df_id_file_match[["FileName", "ParentId"]], on="ParentId", how="right")
        df_order_third = df_order_third[-df_order_third["Symbol"].isna()]
        df_order_third = df_order_third[["T", "Symbol", "Direction", "ExQty", "Exprice", "SysId", "FileName"]]
        df_order_third["Symbol"] = df_order_third["Symbol"].str.strip()
        df_order_third["ExQty"] = df_order_third["ExQty"].astype(float)
        df_order_third["Exprice"] = df_order_third["Exprice"].astype(float)
        df_order_third["Now"] = df_order_third["T"]
        df_order_third["Direction"] = df_order_third["Direction"].apply(lambda x: 1 if x == "买" else -1)
        df_order_third["Server_Name"] = VWAP_CHANNEL_ENUMS.KF_Smart.value
        df_order_third["Cost"] = df_order_third["ExQty"] * df_order_third["Exprice"]

        df_order_third["Vwap_Type"] = df_order_third["FileName"].apply(lambda x: x.split("_")[-1].split(".")[0][:-4])
        df_order_third["OrdVol"] = 0
        return df_order_third

    def __query_third_part_kflocal(self, server, account):
        export_folder = "%s/%s/%s" % (STOCKSELECTION_FOLDER, server, "%s_multi" % self.__date_str.replace("-", ""))
        fund_name = account.split("-")[2]
        match_path = "%s/%s" % (export_folder, "order_map_%s.csv" % fund_name)
        df_id_file_match = pd.read_csv(match_path)
        df_id_file_match.rename(columns={"BasketID": "ParentId"}, inplace=True)

        order_file_path = "%s/%s/%s" % (export_folder, fund_name, "orderAlgo.csv")
        df_order_third = pd.read_csv(
            order_file_path, encoding="utf-8", dtype={"symbol": str, "cumQty": str, "avgPx": str}
        )
        df_order_third = df_order_third[df_order_third["clientName"] == fund_name]

        df_order_third.rename(
            columns={
                "transactTime": "T",
                "symbol": "Symbol",
                "side": "Direction",
                "cumQty": "ExQty",
                "avgPx": "Exprice",
                "clOrdId": "SysId",
                "baskeId": "ParentId",
            },
            inplace=True,
        )
        df_order_third = df_order_third.merge(df_id_file_match[["FileName", "ParentId"]], on="ParentId", how="left")
        df_order_third = df_order_third[["T", "Symbol", "Direction", "ExQty", "Exprice", "SysId", "FileName"]]
        df_order_third["Symbol"] = df_order_third["Symbol"].str.strip()
        df_order_third["Symbol"] = df_order_third["Symbol"].apply(lambda x: x.split(".")[0])
        df_order_third["ExQty"] = df_order_third["ExQty"].astype(float)
        df_order_third["Exprice"] = df_order_third["Exprice"].astype(float)
        df_order_third["Now"] = df_order_third["T"]
        df_order_third = df_order_third[df_order_third["Direction"] != "0"]
        df_order_third = df_order_third[df_order_third["ExQty"] != 0]
        df_order_third["Direction"] = df_order_third["Direction"].apply(lambda x: 1 if x == "B" else -1)
        df_order_third["Server_Name"] = VWAP_CHANNEL_ENUMS.KF_Local.value
        df_order_third["Cost"] = df_order_third["ExQty"] * df_order_third["Exprice"]
        df_order_third["Vwap_Type"] = df_order_third["FileName"].apply(lambda x: x.split("_")[-1].split(".")[0][:-4])
        df_order_third["OrdVol"] = 0
        return df_order_third

    def __query_third_part_kfcs(self, server, account):
        basket_folder_path = "%s/%s/kafang_export/ReportOrderAlgo_%s.dbf" % (
            STOCK_SELECTION_FOLDER,
            server,
            self.__date_str.replace("-", ""),
        )
        order_algo_dbf = DBF(basket_folder_path, encoding="GBK")
        df_order_third = pd.DataFrame(iter(order_algo_dbf))

        export_folder = "%s/%s/%s" % (STOCK_SELECTION_FOLDER, server, "%s_multi" % self.__date_str.replace("-", ""))
        fund_name = account.split("-")[2]
        match_path = "%s/%s" % (export_folder, "order_map_%s.csv" % fund_name)
        df_id_file_match = pd.read_csv(match_path)
        df_id_file_match.rename(columns={"BasketID": "ParentId"}, inplace=True)

        df_order_third = df_order_third[df_order_third["ClientName"] == fund_name]
        df_order_third.rename(
            columns={
                "TransTime": "T",
                "Symbol": "Symbol",
                "Side": "Direction",
                "CumQty": "ExQty",
                "AvgPx": "Exprice",
                "QuoteId": "SysId",
                "BasketId": "ParentId",
            },
            inplace=True,
        )

        df_order_third["ParentId"] = df_order_third["ParentId"].astype(int)
        df_order_third = df_order_third.merge(df_id_file_match[["FileName", "ParentId"]], on="ParentId", how="left")
        df_order_third = df_order_third[["T", "Symbol", "Direction", "ExQty", "Exprice", "SysId", "FileName"]]
        df_order_third["Symbol"] = df_order_third["Symbol"].str.strip()
        df_order_third["Symbol"] = df_order_third["Symbol"].apply(lambda x: x.split(".")[0])
        df_order_third["ExQty"] = df_order_third["ExQty"].astype(float)
        df_order_third["Exprice"] = df_order_third["Exprice"].astype(float)
        df_order_third["Now"] = df_order_third["T"]
        df_order_third = df_order_third[df_order_third["Direction"] != "0"]
        df_order_third = df_order_third[df_order_third["ExQty"] != 0]
        df_order_third["Direction"] = df_order_third["Direction"].apply(lambda x: -1 if x == 2 else 1)
        df_order_third["Server_Name"] = VWAP_CHANNEL_ENUMS.KF_CS.value
        df_order_third["Cost"] = df_order_third["ExQty"] * df_order_third["Exprice"]
        df_order_third["Vwap_Type"] = df_order_third["FileName"].apply(lambda x: x.split("_")[-1].split(".")[0][:-4])
        df_order_third["OrdVol"] = 0
        return df_order_third

    def __query_basket_order_file(self, server_list):
        dict_basket_vwap = {}
        ticker_vol_list = []
        dict_third_fund = {}
        for dict_temp in self.__list_account_third_algo:
            fund_name = dict_temp["account"].split("-")[2]
            dict_third_fund[fund_name] = dict_temp["algo"]

        for server_name in server_list:
            change_save_folder = "%s/%s/%s_change" % (
                STOCK_SELECTION_FOLDER,
                server_name,
                self.__date_str.replace("-", ""),
            )
            for file_name in os.listdir(change_save_folder):
                if not file_name.endswith(".txt"):
                    continue

                with open("%s/%s" % (change_save_folder, file_name)) as fr:
                    vwap_type = file_name.split("-")[-2]
                    vol_type = "%s_Volume" % vwap_type.split("_")[-1][:-3]
                    fund_name = file_name.split("@")[0].split("-")[-1]
                    key_basket = server_name
                    if fund_name in dict_third_fund.keys():
                        key_basket = dict_third_fund[fund_name]
                    temp_list = []
                    for line in fr.readlines():
                        symbol, volume = line.replace("\n", "").strip().split(",")
                        direction = "Buy" if int(volume) > 0 else "Sell"
                        ticker_vol_list.append([key_basket, vol_type, symbol, int(volume), direction])
                        temp_list.append([symbol, int(volume)])
                    temp_list.sort(key=lambda x: x[0])
                    temp_list = ["%s|%s|" % (x[0], str(x[1])) for x in temp_list]
                    key_basket += "".join(temp_list)
                    dict_basket_vwap[key_basket] = vwap_type

        ticker_vol_df = pd.DataFrame(
            ticker_vol_list, columns=["ServerName", "Vol_Type", "Symbol", "Volume", "Direction"]
        )
        ticker_vol_df = ticker_vol_df.pivot_table(
            index=["ServerName", "Symbol", "Direction"], columns="Vol_Type", values="Volume", aggfunc=np.sum
        ).fillna("")

        ticker_vol_df = ticker_vol_df.reset_index()
        self.dict_basket_vwap = dict_basket_vwap
        self.ticker_vol_file_df = ticker_vol_df

    def __build_vwap_report(self):
        stock_order_df = self.__load_basket_order()
        stock_order_df = stock_order_df[
            ["Server_Name", "Symbol", "Direction", "OrdVol", "ExQty", "Exprice", "Cost", "Now", "T", "Vwap_Type"]
        ]

        df_order_third = self.__query_order_third_part()
        df_order_third = df_order_third[stock_order_df.columns]
        stock_order_df = pd.concat([stock_order_df, df_order_third], ignore_index=True, sort=True)
        stock_order_df["ExQty"] = stock_order_df["ExQty"].astype(int)
        stock_order_df["OrdVol"] = stock_order_df["OrdVol"].astype(int)
        stock_order_df["Exprice"] = stock_order_df["Exprice"].astype(float)

        report_file_list = []
        report_list = []
        for (server_name, symbol, direction, vwap_type), df_group in stock_order_df.groupby(
            ["Server_Name", "Symbol", "Direction", "Vwap_Type"]
        ):
            if df_group["ExQty"].sum() != 0:
                vwap_price = df_group["Cost"].sum() / df_group["ExQty"].sum()
            else:
                vwap_price = 0
            price_type = "%s_Ex_Price" % vwap_type
            qty_type = "%s_Ex_Qty" % vwap_type
            report_list.append(
                [
                    server_name,
                    symbol,
                    direction,
                    df_group["ExQty"].sum(),
                    qty_type,
                    vwap_price,
                    price_type,
                    df_group.iloc[-1]["T"],
                ]
            )
        report_df = pd.DataFrame(
            report_list,
            columns=["ServerName", "Symbol", "Direction", "SumQty", "ExQtyType", "VwapPrice", "PriceType", "T_Last"],
        )

        sum_vol_df = (
            report_df[["ServerName", "Symbol", "Direction", "SumQty"]]
            .groupby(["ServerName", "Symbol", "Direction"])
            .sum()
            .reset_index()
        )
        price_df = report_df.pivot_table(
            index=["ServerName", "Symbol", "Direction"], columns=["PriceType"], values=["VwapPrice"], aggfunc=np.average
        ).fillna("")

        price_df.columns = list(map(lambda x: x[1], price_df.columns))
        vol_df = report_df.pivot_table(
            index=["ServerName", "Symbol", "Direction"], columns=["ExQtyType"], values=["SumQty"], aggfunc=np.average
        ).fillna("")
        vol_df.columns = list(map(lambda x: x[1], vol_df.columns))
        t_df = (
            report_df[["ServerName", "Symbol", "Direction", "T_Last"]]
            .groupby(["ServerName", "Symbol", "Direction"])
            .last()
            .reset_index()
        )

        report_df = price_df.merge(vol_df, left_index=True, right_index=True, how="outer").reset_index()
        report_df = report_df.merge(t_df, on=["ServerName", "Symbol", "Direction"])
        report_df = report_df.merge(sum_vol_df, on=["ServerName", "Symbol", "Direction"])
        report_df["Direction"] = report_df["Direction"].apply(lambda x: "Buy" if x > 0 else "Sell")
        report_df = pd.merge(report_df, self.ticker_vol_file_df, how="left", on=["ServerName", "Symbol", "Direction"])
        report_df = pd.merge(report_df, self.__ticker_avg_price_df, how="left", on=["Symbol"])
        # 防止 report_df 为空报错
        report_df = report_df.join(pd.DataFrame(columns=["Performance", "Market_Value", "Pnl"]))
        report_df["T_Last"] = report_df["T_Last"].str.strip()

        for server_name, df_server_report in report_df.groupby("ServerName"):
            df_server_report[["Performance", "Market_Value"]] = df_server_report.apply(
                self.__build_performance, axis=1, result_type="expand"
            )
            list_vwap_period = [x.split("_")[0] for x in [x for x in df_server_report.columns if "Volume" in x]]
            df_server_report["Pnl"] = 0
            for period in list_vwap_period:
                df_server_report["%s_Ex_Percent" % period] = df_server_report[
                    ["%s_Ex_Qty" % period, "%s_Volume" % period]
                ].apply(lambda x: float(x[0]) / abs(float(x[1])) if x[0] != "" and x[1] != "" else 0.0, axis=1)
                df_server_report["%s_Market_Value" % period] = df_server_report[
                    ["%s_Ex_Qty" % period, "%s_Ex_Price" % period]
                ].apply(lambda x: float(x[0]) * abs(float(x[1])) if x[0] != "" and x[1] != "" else 0.0, axis=1)
                df_server_report["%s_Ex_Percent" % period] = df_server_report["%s_Ex_Percent" % period].apply(
                    lambda x: "%.4f%%" % (x * 100)
                )
                df_server_report["%s_Pnl" % period] = df_server_report[
                    ["Direction", "%s_Ex_Qty" % period, "%s_Avg_Price_Market" % period, "%s_Ex_Price" % period]
                ].apply(self.__build_pnl, axis=1)
                df_server_report["Pnl"] = df_server_report["Pnl"] + df_server_report["%s_Pnl" % period]

            columns_format = self.__combine_columns(list_vwap_period)
            df_server_report = df_server_report[columns_format]

            market_value_sum = df_server_report["Market_Value"].sum()
            pnl_sum = df_server_report["Pnl"].sum()
            df_server_report = df_server_report.reset_index(drop=True)
            df_server_report.loc[len(df_server_report)] = np.nan
            df_server_report.at[len(df_server_report) - 1, "Market_Value"] = market_value_sum
            df_server_report.at[len(df_server_report) - 1, "Pnl"] = pnl_sum

            output_folder = os.path.join(DAILY_FILES_FOLDER, "vwap_reports", self.__date_str.replace("-", ""))
            if not os.path.exists(output_folder):
                os.mkdir(output_folder)
            out_put_file_path = "%s/vwap_report_%s.csv" % (output_folder, server_name)
            df_server_report.to_csv(out_put_file_path, index=False, encoding="gbk")
            report_file_list.append(out_put_file_path)
        email_utils.send_attach_email("Vwap成交统计报告", "Date:%s" % self.__date_str, report_file_list)

    @classmethod
    def __combine_columns(cls, list_period):
        columns_meta = [
            "ServerName",
            "Symbol",
            "Direction",
            "_Ex_Price",
            "_Ex_Qty",
            "SumQty",
            "_Volume",
            "_Ex_Percent",
            "_Avg_Price_Market",
            "Performance",
            "_Pnl",
            "Pnl",
            "_Market_Value",
            "Market_Value",
            "T_Last",
        ]
        columns_format = []
        for column in columns_meta:
            if column.startswith("_"):
                for period in list_period:
                    columns_format.append("%s%s" % (period, column))
            else:
                columns_format.append(column)
        return columns_format

    @classmethod
    def __build_performance(cls, row):
        if "1H_Volume" not in row:
            return [0, 0]
        list_vwap_period = map(lambda x: x.split("_")[0], list(filter(lambda x: "Volume" in x, row.index)))
        performance = 0
        market_value = 0
        for vwap_period in list_vwap_period:
            if row["%s_Ex_Qty" % vwap_period] == "":
                continue
            if abs(row["SumQty"]) == 0:
                return [0, 0]

            period_percent = row["%s_Ex_Qty" % vwap_period] / abs(row["SumQty"])

            if row["Direction"] == "Buy":
                performance = (
                    performance
                    + (
                        (row["%s_Avg_Price_Market" % vwap_period] - row["%s_Ex_Price" % vwap_period])
                        / row["%s_Avg_Price_Market" % vwap_period]
                    )
                    * period_percent
                )
                market_value = market_value + abs(row["%s_Ex_Qty" % vwap_period]) * row["%s_Ex_Price" % vwap_period]
            else:
                performance = (
                    performance
                    + (
                        (row["%s_Ex_Price" % vwap_period] - row["%s_Avg_Price_Market" % vwap_period])
                        / row["%s_Avg_Price_Market" % vwap_period]
                    )
                    * period_percent
                )
                market_value = market_value + abs(row["%s_Ex_Qty" % vwap_period]) * row["%s_Ex_Price" % vwap_period]

        return ["%.4f%%" % (performance * 100,), market_value]

    @staticmethod
    def __build_pnl(row):
        if row[1] == "" or row[2] == "":
            return 0
        if row[0] == "Buy":
            # row[1] --> Ex Qty; row[2]-->Market Price; row[3] --> Ex Price
            pnl = row[1] * (float(row[2]) - float(row[3]))
        else:
            pnl = (-1) * row[1] * (float(row[2]) - float(row[3]))
        return pnl

    def __query_algo_config(self):
        dict_algo_config = {}
        algo_config_path = os.path.join(STOCK_SELECTION_FOLDER, "algo_config.txt")
        with open(algo_config_path, "r") as file_algo:
            lines = file_algo.readlines()
            for line in lines:
                if "VWAP" not in line:
                    continue
                items = line.split(",")
                vwap, start_time, end_time = items[0], items[3], items[4]
                dict_algo_config[vwap] = [start_time, end_time]
        self.dict_algo_config = dict_algo_config

    def __query_avg_price_df(self):
        dict_vwap_time = {}
        for vwap_name, list_time in self.dict_algo_config.items():
            vwap_h = vwap_name.split("_")[-1][:-3]
            dict_vwap_time[vwap_h] = list_time

        ticker_list = self.ticker_vol_file_df["Symbol"].values.tolist()
        ticker_list = list(set(ticker_list))
        ticker_avg_list = []
        for file_name in os.listdir(self.__market_data_1min_folder):
            if not file_name.endswith("1min.csv"):
                continue
            ticker = file_name.split("_")[0]
            if ticker not in ticker_list:
                continue
            if ticker.startswith("0") or ticker.startswith("3") or ticker.startswith("6"):
                file_path = "%s/%s" % (self.__market_data_1min_folder, file_name)
                df_1min_info = pd.read_csv(file_path)
                df_1min_info = df_1min_info.set_index("datetime")
                for vwap_type, vwap_time in dict_vwap_time.items():
                    # 全天平均成交价从 wind 拿
                    if "Allday" in vwap_type:
                        continue
                    start_time, end_time = vwap_time
                    start_time = start_time[:-2] + "00"

                    if int(start_time) < 93500:
                        end_time = end_time[:-2] + "00"
                        end_time = "%s:%s:%s" % (end_time[:2], end_time[2:4], end_time[4:6])
                        end_time = "%s %s" % (self.__date_str, end_time)
                        turnover_end = df_1min_info.at[end_time, "turnover"]
                        volume_end = df_1min_info.at[end_time, "volume"]
                        avg_price = turnover_end / volume_end
                    else:
                        end_time = end_time[:-2] + "00"
                        start_time = "%s:%s:%s" % (start_time[:2], start_time[2:4], start_time[4:6])
                        end_time = "%s:%s:%s" % (end_time[:2], end_time[2:4], end_time[4:6])
                        start_time = "%s %s" % (self.__date_str, start_time)
                        end_time = "%s %s" % (self.__date_str, end_time)
                        turnover_start = df_1min_info.at[start_time, "turnover"]
                        turnover_end = df_1min_info.at[end_time, "turnover"]
                        volume_start = df_1min_info.at[start_time, "volume"]
                        volume_end = df_1min_info.at[end_time, "volume"]
                        avg_price = (turnover_end - turnover_start) / (volume_end - volume_start)
                    avg_price_type = "%s_Avg_Price_Market" % vwap_type
                    ticker_avg_list.append([ticker, avg_price_type, avg_price])

        ticker_avg_price_df = pd.DataFrame(ticker_avg_list, columns=["Symbol", "AvgPriceType", "AvgPrice"])
        ticker_avg_price_df = ticker_avg_price_df.pivot_table(
            index="Symbol", columns="AvgPriceType", values="AvgPrice", aggfunc=np.sum
        ).fillna("")
        self.__ticker_avg_price_df = ticker_avg_price_df.reset_index()

        server_model = server_constant.get_server_model("wind_db")
        session_dump_wind = server_model.get_db_session("dump_wind")
        query_sql = (
            "select S_INFO_WINDCODE, S_DQ_AVGPRICE from ASHAREEODPRICES where TRADE_DT = '%s'"
            % self.__date_str.replace("-", "")
        )
        list_avg_price_wind = []
        for x in session_dump_wind.execute(query_sql):
            list_avg_price_wind.append([x[0].split(".")[0], float(x[1])])
        df_avg_price_wind = pd.DataFrame(list_avg_price_wind, columns=["Symbol", "Allday_Avg_Price_Market"])
        self.__ticker_avg_price_df = self.__ticker_avg_price_df.merge(df_avg_price_wind, on="Symbol", how="left")


if __name__ == "__main__":
    pass

    date_str = "2021-09-24"
    load_file_path = "Z:/dailyjob/daily_files/redis_dump/172_16_10_188_1_%s.pickle" % date_str.replace("-", "")
    redis_ip, redis_port, redis_db = const.EOD_CONFIG_DICT["redis_info"]
    import redis

    load_redis_db = redis.StrictRedis(host=redis_ip, port=redis_port, db=5)

    redis_backup_tool = RedisBackupTool()
    redis_backup_tool.load_redis_db(load_redis_db, load_file_path)

    server_list = server_constant.get_servers_by_strategy_group(SERVER_STRATEGY_GROUP_ENUMS.Stock_MultiFactor.value)
    stock_alpha_report_job = StockAlphaReportJob(server_list, date_str)
    stock_alpha_report_job.build_index()
    # stock_alpha_report_job.email_report()
