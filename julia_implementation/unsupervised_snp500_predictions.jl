using CSV
using HTTP
using ZipFile
using DataFrames
using HTMLTables
using PythonCall
using Dates

function read_ff_data()

    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
    # set header
    headers = ["User-Agent" => "Mozilla/5.0"]

    # fetch data
    response = HTTP.get(url, headers)
    bytes = response.body # Vector{UInt8} object

    # unzip
    z = ZipFile.Reader(IOBuffer(bytes))
    csv_file = z.files[1]

    # read csv and load as a DataFrame object

    data_text = String(read(csv_file))
    monthly_data = split(data_text, "\r\n\r\n")[2]
    ff_factors = CSV.read(IOBuffer(monthly_data), DataFrame)
    
    # close file
    close(z)

    return ff_factors
end

function read_sp_data()

    if isfile("../data/sp500"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv")
        return CSV.read("../data/sp500"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv", DataFrame)
    else
        
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        # set header
        headers = ["User-Agent" => "Mozilla/5.0"]

        # fetch data
        response = HTTP.get(url, headers)
        html = String(response.body) # Vector{UInt8} object
    
        # read the html table and load as a DataFrame object
        tables = readtable(html)
        sp500 = DataFrame(tables[1])

        return sp500
    end
end

function get_symbols_list(sp500)
    
    # rename columns for easier handling
    rename!(sp500,
            :1=>:symbol,
            :2=>:security,
            :3=>:sector,
            :4=>:sub_industry,
            :5=>:hq,
            :6=>:date_added,
            :7=>:cik,
            :8=>:founded
           )
    
    symbols_list = unique(sp500[!, :symbol]) # symbols saved as a Vector{String} object

    return symbols_list

end

function get_fin_data(symbols_list)
    """
    get_fin_data function downloads market data for S&P 500 companies
    from the `yfinance` library in Python.
    """
    read_file = true
    if read_file && isfile("../data/yf_data_"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv")
        return CSV.read("../data/yf_data_"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv", DataFrame)
    else
        # This is not working for some reason.
        # Starts the download but then fails all 502 after
        # downloading about 501.
        # Returns an empty dataframe, with all the columns tho.
        # Weird.

        println("downloading data")
    
        yf = pyimport("yfinance")
        
        pyimport("pandas")
        yf_data = yf.download(join(symbols_list, " "), "2026-02-05", "2016-02-05", auto_adjust=false).stack()

        return yf_data
    end
end

function main()
    sp500 = read_sp_data()
    symbols_list = get_symbols_list(sp500)

    yf_data = get_fin_data(symbols_list)
    
    # TODO set date and tickers and index for yf_data
    # then calculate the following metrics:
    # garman klass volatility
    # rsi
    # bb low, bb medium, bb high
    # atr
    # macd
    # dollar volume
end

main()
