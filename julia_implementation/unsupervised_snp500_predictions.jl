using CSV
using HTTP
using ZipFile
using DataFrames
using HTMLTables
using PythonCall
using Dates
using Statistics

function read_ff_data()

    if isfile("../data/ff_factors.csv")
        return CSV.read("ff_factors.csv")

    else
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
end

function read_sp_data()

    today_str = Dates.format(today(), dateformat"yyyy-mm-dd")

    if isfile("../data/sp500_"*today_str*".csv")
        return CSV.read("../data/sp500_"*today_str*".csv", DataFrame, drop=["Column1"])
    else

        println("latest sp500 data not found. attempting downlaod. you should probably run get_data.py")
        
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        # set header
        headers = ["User-Agent" => "Mozilla/5.0"]

        # fetch data
        response = HTTP.get(url, headers)
        html = String(response.body) # Vector{UInt8} object
    
        # read the html table and load as a DataFrame object
        tables = readtable(html)
        sp500 = DataFrame(tables[1])
    
        # save the downloaded S&P data
        CSV.write("../data/sp500_"*today_str*".csv", sp500)

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

    symbols_list = unique(replace.(sp500[!, :symbol], "."=>"-"))

    return symbols_list

end

function get_fin_data(symbols_list)
    """
    downloads market data for S&P 500 companies
    using the `yfinance` library in Python.
    """

    read_file = true # SET TO TRUE IF YOU DON'T WANT TO DOWNLOAD DATA

    if read_file && isfile("../data/yf_data_"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv")
        return CSV.read("../data/yf_data_"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv", DataFrame)
    else
        # This is not working for some reason.
        # Starts the download but then fails all 502 after
        # downloading about 501.
        # Returns an empty dataframe, with all the columns tho.
        # Weird.
        #
        println("latest yf data not found. attempting downlaod. you should probably run get_data.py")

        println("downloading data")
    
        yf = pyimport("yfinance")
        pd = pyimport("pandas")

        data_frames = []
        for i in 1:50:length(symbols_list)
            chunk = symbols_list[i:min(i+49, end)]
            push!(data_frames, yf.download(join(chunk, " "), "2016-02-05", "2026-02-05", auto_adjust=false).stack())
        end
        yf_data = pd.concat(data_frames)
        
        CSV.write("../data/yf_data_"*Dates.format(today(), dateformat"yyyy-mm-dd")*".csv", yf_data)

        return yf_data
    end
end


function gkv(
        Open::AbstractVector,
        High::AbstractVector,
        Low::AbstractVector,
        Close::AbstractVector
    )::AbstractVector
    """
    Garman-Klass volatility estimator calculation.
    """
    
    open = log.(Open)
    high = log.(High)
    low = log.(Low)
    close = log.(Close)

    return sqrt.(0.5 .* (high .- low).^2 .- (2*log(2) - 1) .* (close .- open).^2)
end

function ema(x::AbstractVector; period::Int, wilders::Bool=false)::AbstractVector
    """
    Exponential Moving Average (EMA) calculation.
    """
    if wilders
        alpha = 1/period # Wilder's Smoothing (RMA) uses a different alpha
    else
        alpha = 2/(period+1)
    end
    n = length(x)

    averages = similar(x, Float64)
    averages[1] = x[1]

    for t in 2:n
        averages[t] = alpha * x[t] + (1-alpha)*averages[t-1]
    end

    return averages
end

function atr(
        High::AbstractVector,
        Low::AbstractVector,
        Close::AbstractVector;
        period::Int=14
    )::AbstractVector
    """
    Average True Range (ATR) calculation.
    """

    n = length(High)
    tr = similar(High)

    tr[1] = High[1] - Low[1] # first period

    for t in 2:n
        tr[t] = max(
                    High[t]-Low[t],
                    abs(High[t]-Close[t-1]),
                    abs(Low[t]-Close[t-1])
                    )
    end

    atr_values = ema(tr, wilders=true, period=period)
    
    return atr_values
end



function macd(
        price::AbstractVector,
        fast::Int=12,
        slow::Int=26,
        signal::Int=9
    )::Tuple{AbstractVector, AbstractVector}
    """
    Moving Average Convergence Divergence (MACD) calculation.
    """

    ema_fast = ema(price, period=fast)
    ema_slow = ema(price, period=slow)

    macd_line = ema_fast .- ema_slow
    signal_line = ema(macd_line, period=signal)
    
    return macd_line, signal_line

end

function rolling_zscore(x, window)::AbstractVector
    """
    Rolling z-score calculation.
    """
    z = similar(x)
    z .= NaN
    for t in window:length(x)
        mu = mean(@view x[t-window+1:t])
        sigma = std(@view x[t-window+1:t])
        z[t] = (x[t]-mu)/sigma
    end
    return z
end

function rsi(
        price::AbstractVector; period::Int=14
    )::Tuple{AbstractVector, AbstractVector}
    """
    Relative Strength Index (RSI) calculation.
    """

    n = length(price)
    rsi = fill(NaN, n)

    price_delta = diff(price)

    gains = max.(price_delta, 0.0)
    losses = max.(-price_delta, 0.0)

    # initialise averages and rsi
    avg_gain = mean(gains[1:period])
    avg_loss = mean(losses[1:period])

    rs = avg_loss == 0 ? Inf : avg_gain/avg_loss
    rsi[period+1] = 100 - 100 / (1+rs)

    for t in (period+2):n
        avg_gain = (avg_gain * (period-1) + gains[t-1])/period
        avg_loss = (avg_loss * (period-1) + losses[t-1])/period

        rs = avg_loss == 0 ? Inf : avg_gain/avg_loss
        rsi[t] = 100-100/(1+rs)
    end

    centered_rsi = rsi .- 50

    return rsi, centered_rsi
end


function main()
    sp500 = read_sp_data()
    symbols_list = get_symbols_list(sp500)

    yf_data = get_fin_data(symbols_list)

    
    # TODO set date and tickers and index for yf_data
    
    # then calculate the following metrics:
    # Volatility metric
      # PCA of
        # garman klass volatility
        # atr
        # macd
    # rsi
    # percentage bandwidth (what?) | bb low, bb medium, bb high
    # log(dollar volume)
end

main()
