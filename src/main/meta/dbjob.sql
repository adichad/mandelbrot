USE [FastSynchronization];
-- prepare environment
IF OBJECT_ID (N'[eDMS].[udfCleanupString]', N'FN') IS NOT NULL
    DROP FUNCTION [eDMS].[udfCleanupString];
GO

CREATE FUNCTION [eDMS].[udfCleanupString](@input varchar(max))
RETURNS varchar(max) 
AS 
BEGIN
    DECLARE @RET varchar(max);
    SELECT @RET = @input;
	SELECT @RET = REPLACE(@RET, char(0), '');
	SELECT @RET = REPLACE(@RET, char(13), ' ');
	SELECT @RET = REPLACE(@RET, char(10), ' ');
	SELECT @RET = REPLACE(@RET, char(9), ' ');
	SELECT @RET = REPLACE(@RET, char(12), ' ');
	SELECT @RET = REPLACE(@RET, char(11), ' ');
	SELECT @RET = REPLACE(@RET, '"', '\"');
    RETURN LTRIM(RTRIM(@RET));
END;
GO

IF OBJECT_ID (N'[eDMS].[udfFilterSlug]', N'FN') IS NOT NULL
    DROP FUNCTION [eDMS].[udfFilterSlug];
GO
CREATE FUNCTION [eDMS].[udfFilterSlug](@input varchar(max))
RETURNS varchar(max)
AS
BEGIN
    DECLARE @RET varchar(max);
	SELECT @RET = @input;
	SELECT @RET = REPLACE(@RET, '-', ' ');
	SELECT @RET = REPLACE(@RET, '&', ' ');
	SELECT @RET = REPLACE(@RET, '"', ' ');
	SELECT @RET = REPLACE(@RET, '_', ' ');
	SELECT @RET = REPLACE(@RET, '@', ' ');
	SELECT @RET = REPLACE(@RET, '(', ' ');
	SELECT @RET = REPLACE(@RET, ')', ' ');
	SELECT @RET = REPLACE(@RET, ',', ' ');
	SELECT @RET = REPLACE(@RET, '"', ' ');
	SELECT @RET = REPLACE(@RET, '/', ' ');
	SELECT @RET = REPLACE(@RET, '\', ' ');
	SELECT @RET = REPLACE(@RET, '[', ' ');
	SELECT @RET = REPLACE(@RET, ']', ' ');
	SELECT @RET = REPLACE(@RET, '<', ' ');
	SELECT @RET = REPLACE(@RET, '>', ' ');
	SELECT @RET = REPLACE(@RET, '{', ' ');
	SELECT @RET = REPLACE(@RET, '}', ' ');
	SELECT @RET = REPLACE(@RET, '@', ' ');
	SELECT @RET = REPLACE(@RET, '.', ' ');
	SELECT @RET = REPLACE(@RET, ';', ' ');
	SELECT @RET = REPLACE(@RET, '%', ' ');


	SELECT @RET = REPLACE(@RET, char(0), ' ');
	SELECT @RET = REPLACE(@RET, char(13), ' ');
	SELECT @RET = REPLACE(@RET, char(10), ' ');
	SELECT @RET = REPLACE(@RET, char(9), ' ');
	SELECT @RET = REPLACE(@RET, char(12), ' ');
	SELECT @RET = REPLACE(@RET, char(11), ' ');
	-- '      ' -> '<><><><><><>' -> '<>' -> '-'
	SELECT @RET = replace(replace(replace(LTRIM(RTRIM(@RET)),' ','<>'),'><',''),'<>','-');
	RETURN LOWER(@RET);
END;
GO


IF OBJECT_ID('[FastSynchronization].[eDMS].[BizLocSearch_Staging]') IS NULL
BEGIN

CREATE TABLE [FastSynchronization].[eDMS].[BizLocSearch_Staging](
    [RowID] [int] not null
   ,[UniqueID] [varchar](100) unique NOT NULL
   ,[ULID] [varchar](30) not null
   ,[BusinessUserID] [int] NOT NULL
   ,[ProductID] [int] NOT NULL
   ,[LocationID] [int] NOT NULL
   ,[BusinessCompanyID] [int] NULL
   ,[L3CategoryID] [int] NULL
   ,[DetailSlug] [varchar](7000) NULL
   ,[UpdateFlag] [bit] NULL
   ,[InsertFlag] [bit] NULL
   ,[DeleteFlag] [bit] NULL
   ,[LastUpdated] [datetime] NOT NULL
   ,[ValidFlag] [bit] NULL
   ,[PPCOnline] [int] NULL
   ,[PPCVoice] [int] NULL
   ,[CustomerType] [int] NULL
   ,[NowCustomerType] [int] NULL
   ,[LocationName] [varchar](500) NULL
   ,[LocationDescription] [varchar](7000) NULL
   ,[LocationType] [varchar](500) NULL
   ,[BusinessType] [varchar](1600) NULL
   ,[Address] [varchar](1502) NULL
   ,[Area] [varchar](7000) NULL
   ,[AreaSlug] [varchar](7000) NULL
   ,[AreaSynonyms] [varchar](1000) NULL
   ,[City] [varchar](7000) NULL
   ,[CitySlug] [varchar](7000) NULL
   ,[CitySynonyms] [varchar](7000) NULL
   ,[PinCode] [varchar](100) NULL
   ,[ZonesServed] [varchar](500) NULL
   ,[Zone] [varchar](7000) NULL
   ,[State] [varchar](7000) NULL
   ,[Country] [varchar](7000) NULL
   ,[Latitude] [float] NULL
   ,[Longitude] [float] NULL
   ,[CompanyName] [varchar](500) NULL
   ,[CompanyAliases] [varchar](500) NULL
   ,[CompanyDescription] [varchar](max) NULL
   ,[CompanyKeywords] [varchar](max) NULL
   ,[CompanyLogo] [varchar](500) NULL
   ,[AdvertiserURL] [varchar](200) NULL
   ,[PrimaryContactName] [varchar](7000) NULL
   ,[PrimaryContactMobile] [varchar](7000) NULL
   ,[PrimaryContactEmail] [varchar](7000) NULL
   ,[LocationMobile] [varchar](7000) NULL
   ,[LocationLandLine] [varchar](7000) NULL
   ,[LocationEmail] [varchar](7000) NULL
   ,[LocationDIDNumber] [varchar](7000) NULL
   ,[L1Category] [varchar](7000) NULL
   ,[L2Category] [varchar](7000) NULL
   ,[L3Category] [varchar](7000) NULL
   ,[L3CategorySlug] [varchar](7000) NULL
   ,[CategoryPath] [varchar](7000) NULL
   ,[CategoryKeywords] [varchar](7000) NULL
   ,[A1] [varchar](7000) NULL
   ,[A2] [varchar](7000) NULL
   ,[A3] [varchar](7000) NULL
   ,[A4] [varchar](7000) NULL
   ,[A5] [varchar](7000) NULL
   ,[A6] [varchar](7000) NULL
   ,[A7] [varchar](7000) NULL
   ,[A8] [varchar](7000) NULL
   ,[A9] [varchar](7000) NULL
   ,[A10] [varchar](7000) NULL
   ,[A11] [varchar](7000) NULL
   ,[A12] [varchar](7000) NULL
   ,[A13] [varchar](7000) NULL
   ,[A14] [varchar](7000) NULL
   ,[A15] [varchar](7000) NULL
   ,[A16] [varchar](7000) NULL
   ,[A17] [varchar](7000) NULL
   ,[A18] [varchar](7000) NULL
   ,[A19] [int] NULL
   ,[A20] [int] NULL
   ,[ProductName] [varchar](7000) NULL
   ,[ProductDescription] [varchar](7000) NULL
   ,[ProductBrand] [varchar](7000) NULL
   ,[ProductImages] [varchar](7000) NULL
   ,[Q1] [varchar](100) NULL
   ,[Q2] [varchar](100) NULL
   ,[Q3] [varchar](100) NULL
   ,[Q4] [varchar](100) NULL
   ,[Q5] [varchar](100) NULL
   ,[Q6] [varchar](100) NULL
   ,[Q7] [varchar](100) NULL
   ,[Q8] [varchar](100) NULL
   ,[Q9] [varchar](100) NULL
   ,[Q10] [varchar](100) NULL
   ,[Q11] [varchar](100) NULL
   ,[Q12] [varchar](100) NULL
   ,[Q13] [varchar](100) NULL
   ,[Q14] [varchar](100) NULL
   ,[Q15] [varchar](100) NULL
   ,[Q16] [varchar](100) NULL
   ,[Q17] [varchar](100) NULL
   ,[Q18] [varchar](100) NULL
   ,[Q19] [varchar](100) NULL
   ,[Q20] [varchar](100) NULL
   ) on [PRIMARY];

   CREATE CLUSTERED INDEX [BizLocSearch_Staging__ClusteredIndex_rowid] ON [FastSynchronization].[eDMS].[BizLocSearch_Staging]
   (
      [RowID] ASC
   ) on [PRIMARY];

END


IF OBJECT_ID('[FastSynchronization].[eDMS].[BizLocSearch_Bookmark]') IS NULL
CREATE TABLE [FastSynchronization].[eDMS].[BizLocSearch_Bookmark] (
  [ID] int IDENTITY(1,1) PRIMARY KEY not null
 ,[BookmarkDT] [datetime] not null
 ,[JobStartDT] [datetime] not null
 ,[JobEndDT] [datetime] not null
 ,[RowCount] int not null
)

IF OBJECT_ID('[FastSynchronization].[eDMS].[BizLocSearchID_Staging]') IS NULL
BEGIN
  CREATE TABLE [FastSynchronization].[eDMS].[BizLocSearchID_Staging](
      [RowID] [int] identity(1,1) not null
     ,[BusinessUserID] [int] NOT NULL
	 ,[LocationID] [int] NOT NULL
     ,[LastUpdatedDate] [datetime]
  ) ON [PRIMARY];
  CREATE UNIQUE CLUSTERED INDEX [BizLocSearchID_Staging__NonClusteredIndex_ids] ON [FastSynchronization].[eDMS].[BizLocSearchID_Staging]
  (
      [BusinessUserID] ASC
	 ,[LocationID] ASC
  ) 
  WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) 
  ON [PRIMARY];
  CREATE NONCLUSTERED INDEX [BizLocSearchID_Staging__NonClusteredIndex_date] ON [FastSynchronization].[eDMS].[BizLocSearchID_Staging]
  (
      [LastUpdatedDate] DESC
  ) 
  WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) 
  ON [PRIMARY];
END


---------------------------
-- begin data movement

DECLARE @BatchSize int
	   ,@BasePath varchar(128)
	   ,@FilePrefix varchar(7000)
	   ,@DestPath varchar(128)

SET @BatchSize = 50000
SET @BasePath = 'f:\mandelbrot-tmp'
SET @FilePrefix = 'bizloc'
SET @DestPath = 'f:\mandelbrot'

DECLARE @FromBookmarkDT [datetime]
       ,@MaxSourceDT [datetime]
       ,@ToBookmarkDT [datetime]
	   ,@NextBookmarkDT [datetime]
	   ,@FromRowID [bigint]
	   ,@ToRowID [bigint]
	   ,@query [varchar](max)
	   ,@JobStartTime datetime = GETDATE()


-- Fetch last bookmark
select @FromBookmarkDT = cast(coalesce(max([BookmarkDT]), '2004-01-01 00:00:00') as [DateTime]) from [FastSynchronization].[eDMS].[BizLocSearch_Bookmark]

select @MaxSourceDT = max(upddate) from [10.0.4.43].[FastSynchronization].[eDMS].[GetitFlatfile] a (nolock)

IF @MaxSourceDT > @FromBookmarkDT
BEGIN

MERGE INTO [eDMS].[GetitFlatfile_static] AS gff
USING (          select a.* 
                   from  [10.0.4.43].[FastSynchronization].[eDMS].[GetitFlatfile] a (nolock)
       right outer join (  select a.yusrid, a.intgen1, a.batvypid, max(a.upddate) as upddate 
	                         from [10.0.4.43].[FastSynchronization].[eDMS].[GetitFlatfile] a (nolock)
	                        where a.upddate >= @FromBookmarkDT
	                     group by a.yusrid, a.intgen1, a.batvypid) b
	                 on a.yusrid = b.yusrid
					and a.intgen1 = b.intgen1
					and a.batvypid = b.batvypid
				    and a.upddate = b.upddate) as id
  ON gff.yusrid = id.yusrid
 AND gff.intgen1 = id.intgen1
 AND gff.batvypid = id.batvypid
WHEN MATCHED THEN
    UPDATE SET
	gff.[yclttyp] = id.[yclttyp],--
	gff.[gnyclttyp] = id.[gnyclttyp],--
	--gff.[yusrid] = id.[yusrid],--
	gff.[ymtloc] = id.[ymtloc],--
	gff.[batvycoid] = id.[batvycoid],--
	gff.[ybstyp] = id.[ybstyp],--
	gff.[yconam] = id.[yconam],--
	gff.[cimglogo1] = id.[cimglogo1],--
	gff.[ysrc] = id.[ysrc],--
	gff.[ysrcid] = id.[ysrcid],--
	gff.[deleteflag] = id.[deleteflag],--
	gff.[yvalid] = id.[yvalid],--
	gff.[ycodec] = id.[ycodec],--
	gff.[ypsupkey] = id.[ypsupkey],
	gff.[yratng] = id.[yratng],
	gff.[sgen44] = id.[sgen44],
	gff.[comaprvd] = id.[comaprvd],
	gff.[yladd] = id.[yladd],
	gff.[ylbdnam] = id.[ylbdnam],
	gff.[ydftloc] = id.[ydftloc],
	gff.[ylem] = id.[ylem],
	gff.[id] = id.[id],
	gff.[ylmmilat] = id.[ylmmilat],
	gff.[lat] = id.[lat],
	gff.[ylloc] = id.[ylloc],
	gff.[ylmmilong] = id.[ylmmilong],
	gff.[lon] = id.[lon],
	gff.[ypoarsrv] = id.[ypoarsrv],
	gff.[cpin] = id.[cpin],
	gff.[comlocdelflag] = id.[comlocdelflag],
	gff.[ylsloc] = id.[ylsloc],
	gff.[yltyp] = id.[yltyp],
	gff.[ccty] = id.[ccty],
	gff.[ylcry] = id.[ylcry],
	gff.[cstate] = id.[cstate],
	gff.[czone] = id.[czone],
	gff.[ylptcem] = id.[ylptcem],
	gff.[ylptcmob] = id.[ylptcmob],
	gff.[ylptcnam] = id.[ylptcnam],
	gff.[yarsyn] = id.[yarsyn],
	gff.[ypbrdnam] = id.[ypbrdnam],
	gff.[ypnam] = id.[ypnam],
	--[batvypid],
	gff.[ypdesc] = id.[ypdesc],
	gff.[prodelflag] = id.[prodelflag],
	gff.[cimg1] = id.[cimg1],
	gff.[ypcat3id] = id.[ypcat3id],
	gff.[ypcat3] = id.[ypcat3],
	gff.[ypcat2] = id.[ypcat2],
	gff.[ypcat1] = id.[ypcat1],
	gff.[ypcat3a19] = id.[ypcat3a19],
	gff.[ypcat3a20] = id.[ypcat3a20],
	gff.[ypcat3a1] = id.[ypcat3a1],
	gff.[ypcat3a10] = id.[ypcat3a10],
	gff.[ypcat3a11] = id.[ypcat3a11],
	gff.[ypcat3a12] = id.[ypcat3a12],
	gff.[ypcat3a13] = id.[ypcat3a13],
	gff.[ypcat3a14] = id.[ypcat3a14],
	gff.[ypcat3a15] = id.[ypcat3a15],
	gff.[ypcat3a16] = id.[ypcat3a16],
	gff.[ypcat3a17] = id.[ypcat3a17],
	gff.[ypcat3a18] = id.[ypcat3a18],
	gff.[ypcat3a2] = id.[ypcat3a2],
	gff.[ypcat3atr3] = id.[ypcat3atr3],
	gff.[ypcat3a4] = id.[ypcat3a4],
	gff.[ypcat3a5] = id.[ypcat3a5],
	gff.[ypcat3a6] = id.[ypcat3a6],
	gff.[ypcat3a7] = id.[ypcat3a7],
	gff.[ypcat3a8] = id.[ypcat3a8],
	gff.[ypcat3a9] = id.[ypcat3a9],
	gff.[pvonh] = id.[pvonh],
	gff.[pvonp] = id.[pvonp],
	gff.[pvvoh] = id.[pvvoh],
	gff.[pvvop] = id.[pvvop],
	gff.[upddate] = id.[upddate],
	gff.[cmnscat] = id.[cmnscat],
	gff.[ylmob1] = id.[ylmob1],
	gff.[ylph1] = id.[ylph1],
	gff.[ylwst1] = id.[ylwst1],
	--gff.[ypbusinessid] = id.[ypbusinessid],
	gff.[ypcat3keys] = id.[ypcat3keys],
	gff.[ypcat3phr] = id.[ypcat3phr],
	gff.[ypcat3stwrd] = id.[ypcat3stwrd],
	gff.[ypcat3syn] = id.[ypcat3syn],
	gff.[ycat3sepkey] = id.[ycat3sepkey],
	gff.[igen1] = id.[igen1],
	gff.[sgen48] = id.[sgen48],
	gff.[rrvw] = id.[rrvw],
	gff.[CPLTYPE] = id.[CPLTYPE],
	gff.[espNew] = id.[espNew],
	gff.[espUpdate] = id.[espUpdate],
	gff.[espDelete] = id.[espDelete],
	gff.[espStatusNM] = id.[espStatusNM],
	gff.[espUpdDateNM] = id.[espUpdDateNM],
	gff.[espDbUpdDate] = id.[espDbUpdDate],
	gff.[espStatusSify] = id.[espStatusSify],
	gff.[espUpdDateSify] = id.[espUpdDateSify],
	gff.[strGen1] = id.[strGen1],
	gff.[gmproducts] = id.[gmproducts],
	gff.[strGen2] = id.[strGen2],
	--[intgen1],
	gff.[fsubdisdet] = id.[fsubdisdet],
	gff.[gmAmenities] = id.[gmAmenities],
	gff.[gmcategories] = id.[gmcategories],
	gff.[flogid] = id.[flogid],
	gff.[fcatid] = id.[fcatid],
	gff.[gmbrands] = id.[gmbrands],
	gff.[strGen3] = id.[strGen3],
	gff.[Fow] = id.[Fow]
WHEN NOT MATCHED THEN 
      INSERT (	[rown],--
	[yclttyp],--
	[gnyclttyp],--
	[yusrid],--
	[ymtloc],--
	[batvycoid],--
	[ybstyp],--
	[yconam],--
	[cimglogo1],--
	[ysrc],--
	[ysrcid],--
	[deleteflag],--
	[yvalid],--
	[ycodec],--
	[ypsupkey],
	[yratng],
	[sgen44],
	[comaprvd],
	[yladd],
	[ylbdnam],
	[ydftloc],
	[ylem],
	[id],
	[ylmmilat],
	[lat],
	[ylloc],
	[ylmmilong],
	[lon],
	[ypoarsrv],
	[cpin],
	[comlocdelflag],
	[ylsloc],
	[yltyp],
	[ccty],
	[ylcry],
	[cstate],
	[czone],
	[ylptcem],
	[ylptcmob],
	[ylptcnam],
	[yarsyn],
	[ypbrdnam],
	[ypnam],
	[batvypid],
	[ypdesc],
	[prodelflag],
	[cimg1],
	[ypcat3id],
	[ypcat3],
	[ypcat2],
	[ypcat1],
	[ypcat3a19],
	[ypcat3a20],
	[ypcat3a1],
	[ypcat3a10],
	[ypcat3a11],
	[ypcat3a12],
	[ypcat3a13],
	[ypcat3a14],
	[ypcat3a15],
	[ypcat3a16],
	[ypcat3a17],
	[ypcat3a18],
	[ypcat3a2],
	[ypcat3atr3],
	[ypcat3a4],
	[ypcat3a5],
	[ypcat3a6],
	[ypcat3a7],
	[ypcat3a8],
	[ypcat3a9],
	[pvonh],
	[pvonp],
	[pvvoh],
	[pvvop],
	[upddate],
	[cmnscat],
	[ylmob1],
	[ylph1],
	[ylwst1],
	[ypbusinessid],
	[ypcat3keys],
	[ypcat3phr],
	[ypcat3stwrd],
	[ypcat3syn],
	[ycat3sepkey],
	[igen1],
	[sgen48],
	[rrvw],
	[CPLTYPE],
	[espNew],
	[espUpdate],
	[espDelete],
	[espStatusNM],
	[espUpdDateNM],
	[espDbUpdDate],
	[espStatusSify],
	[espUpdDateSify],
	[strGen1],
	[gmproducts],
	[strGen2],
	[intgen1],
	[fsubdisdet],
	[gmAmenities],
	[gmcategories],
	[flogid],
	[fcatid],
	[gmbrands],
	[strGen3],
	[Fow])
      VALUES (id.[rown],--
	id.[yclttyp],--
	id.[gnyclttyp],--
	id.[yusrid],--
	id.[ymtloc],--
	id.[batvycoid],--
	id.[ybstyp],--
	id.[yconam],--
	id.[cimglogo1],--
	id.[ysrc],--
	id.[ysrcid],--
	id.[deleteflag],--
	id.[yvalid],--
	id.[ycodec],--
	id.[ypsupkey],
	id.[yratng],
	id.[sgen44],
	id.[comaprvd],
	id.[yladd],
	id.[ylbdnam],
	id.[ydftloc],
	id.[ylem],
	id.[id],
	id.[ylmmilat],
	id.[lat],
	id.[ylloc],
	id.[ylmmilong],
	id.[lon],
	id.[ypoarsrv],
	id.[cpin],
	id.[comlocdelflag],
	id.[ylsloc],
	id.[yltyp],
	id.[ccty],
	id.[ylcry],
	id.[cstate],
	id.[czone],
	id.[ylptcem],
	id.[ylptcmob],
	id.[ylptcnam],
	id.[yarsyn],
	id.[ypbrdnam],
	id.[ypnam],
	id.[batvypid],
	id.[ypdesc],
	id.[prodelflag],
	id.[cimg1],
	id.[ypcat3id],
	id.[ypcat3],
	id.[ypcat2],
	id.[ypcat1],
	id.[ypcat3a19],
	id.[ypcat3a20],
	id.[ypcat3a1],
	id.[ypcat3a10],
	id.[ypcat3a11],
	id.[ypcat3a12],
	id.[ypcat3a13],
	id.[ypcat3a14],
	id.[ypcat3a15],
	id.[ypcat3a16],
	id.[ypcat3a17],
	id.[ypcat3a18],
	id.[ypcat3a2],
	id.[ypcat3atr3],
	id.[ypcat3a4],
	id.[ypcat3a5],
	id.[ypcat3a6],
	id.[ypcat3a7],
	id.[ypcat3a8],
	id.[ypcat3a9],
	id.[pvonh],
	id.[pvonp],
	id.[pvvoh],
	id.[pvvop],
	id.[upddate],
	id.[cmnscat],
	id.[ylmob1],
	id.[ylph1],
	id.[ylwst1],
	id.[ypbusinessid],
	id.[ypcat3keys],
	id.[ypcat3phr],
	id.[ypcat3stwrd],
	id.[ypcat3syn],
	id.[ycat3sepkey],
	id.[igen1],
	id.[sgen48],
	id.[rrvw],
	id.[CPLTYPE],
	id.[espNew],
	id.[espUpdate],
	id.[espDelete],
	id.[espStatusNM],
	id.[espUpdDateNM],
	id.[espDbUpdDate],
	id.[espStatusSify],
	id.[espUpdDateSify],
	id.[strGen1],
	id.[gmproducts],
	id.[strGen2],
	id.[intgen1],
	id.[fsubdisdet],
	id.[gmAmenities],
	id.[gmcategories],
	id.[flogid],
	id.[fcatid],
	id.[gmbrands],
	id.[strGen3],
	id.[Fow]);



-- Get Delta UL ids and upddates to process
TRUNCATE TABLE [FastSynchronization].[eDMS].[BizLocSearchID_Staging]

INSERT INTO [FastSynchronization].[eDMS].[BizLocSearchID_Staging] ([BusinessUserID], [LocationID], [LastUpdatedDate])
     SELECT 
            [yusrid] as [BusinessUserID]
           ,[intgen1] as [LocationID]
           ,max([upddate]) as [LastUpdatedDate]
       FROM [FastSynchronization].[eDMS].[GetitFlatfile_Static]
      WHERE [upddate] >= @FromBookmarkDT
   GROUP BY [yusrid], [intgen1]
   ORDER BY [yusrid] ASC, [intgen1] ASC
declare @TotalRows bigint = @@ROWCOUNT
SELECT @TotalRows as [BizLocSearchID_Staging]

-- find next bookmark
SELECT @NextBookmarkDT = MAX([LastUpdatedDate]) FROM [FastSynchronization].[eDMS].[BizLocSearchID_Staging]


-- fetch full records for delta UL ids
TRUNCATE TABLE [FastSynchronization].[eDMS].[BizLocSearch_Staging];

insert into [FastSynchronization].[eDMS].[BizLocSearch_Staging]
(
    [RowID]
   ,[UniqueID]
   ,[ULID]
   ,[BusinessUserID]
   ,[ProductID]
   ,[LocationID]
   ,[BusinessCompanyID]
   ,[L3CategoryID]
   ,[DetailSlug]
   ,[UpdateFlag]
   ,[InsertFlag]
   ,[DeleteFlag]
   ,[LastUpdated]
   ,[ValidFlag]
   ,[PPCOnline]
   ,[PPCVoice]
   ,[CustomerType]
   ,[NowCustomerType]
   ,[LocationName]
   ,[LocationDescription]
   ,[LocationType]
   ,[BusinessType]
   ,[Address]
   ,[Area]
   ,[AreaSlug]
   ,[AreaSynonyms]
   ,[City]
   ,[CitySlug]
   ,[CitySynonyms]
   ,[PinCode]
   ,[ZonesServed]
   ,[Zone]
   ,[State]
   ,[Country]
   ,[Latitude]
   ,[Longitude]
   ,[CompanyName]
   ,[CompanyAliases]
   ,[CompanyDescription]
   ,[CompanyKeywords]
   ,[CompanyLogo]
   ,[AdvertiserURL]
   ,[PrimaryContactName]
   ,[PrimaryContactMobile]
   ,[PrimaryContactEmail]
   ,[LocationMobile]
   ,[LocationLandLine]
   ,[LocationEmail]
   ,[LocationDIDNumber]
   ,[L1Category]
   ,[L2Category]
   ,[L3Category]
   ,[L3CategorySlug]
   ,[CategoryPath]
   ,[CategoryKeywords]
   ,[A1]
   ,[A2]
   ,[A3]
   ,[A4]
   ,[A5]
   ,[A6]
   ,[A7]
   ,[A8]
   ,[A9]
   ,[A10]
   ,[A11]
   ,[A12]
   ,[A13]
   ,[A14]
   ,[A15]
   ,[A16]
   ,[A17]
   ,[A18]
   ,[A19]
   ,[A20]
   ,[ProductName]
   ,[ProductDescription]
   ,[ProductBrand]
   ,[ProductImages]
   ,[Q1]
   ,[Q2]
   ,[Q3]
   ,[Q4]
   ,[Q5]
   ,[Q6]
   ,[Q7]
   ,[Q8]
   ,[Q9]
   ,[Q10]
   ,[Q11]
   ,[Q12]
   ,[Q13]
   ,[Q14]
   ,[Q15]
   ,[Q16]
   ,[Q17]
   ,[Q18]
   ,[Q19]
   ,[Q20]
)
select * 
from (
           SELECT 
		          -- ids
				  ids.[RowID] as [RowID]
				 ,[ypbusinessid] as [UniqueId]
                 ,concat('U',cast([yusrid] as varchar(13)),'L',cast([intgen1] as varchar(13))) as [ULID]
				 ,[yusrid] as [BusinessUserID]
				 ,[batvypid] as [ProductID]
                 ,[intgen1] as [LocationID]
                 ,[batvycoid] as [BusinessCompanyID]
				 ,[ypcat3id] as [L3CategoryID]
				 ,concat([eDMS].[udfFilterSlug](a.[ccty]), '/', [eDMS].[udfFilterSlug](a.[yconam]), '-', [eDMS].[udfFilterSlug](a.ylloc), '-', [eDMS].[udfFilterSlug](a.ccty), '-U', cast(a.[yusrid] as varchar(13)), 'L', cast(a.[intgen1] as varchar(13))) as [DetailSlug]
				 -- status
				 ,[espUpdate] as [UpdateFlag] 
				 ,[espNew] as [InsertFlag]
				 ,[espDelete] as [DeleteFlag]
				 ,[upddate] as [LastUpdated]
				 -- signals
				 ,[yvalid] as [ValidFlag]
                 ,[pvonp] as [PPCOnline]
                 ,[pvvoh] as [PPCVoice]
                 ,[yclttyp] as [CustomerType]
				 ,[gnyclttyp] as [NowCustomerType]
				 -- location
				 ,[eDMS].[udfCleanupString](a.[yconam]) as [LocationName] -- should be ylconam!
                 ,[eDMS].[udfCleanupString]([ypdesc]) as [LocationDescription]
                 ,[eDMS].[udfCleanupString]([yltyp]) as [LocationType]
                 ,[eDMS].[udfCleanupString]([ybstyp]) as [BusinessType]
                 ,[eDMS].[udfCleanupString](concat(isnull([yladd],''),' ', isnull([ylbdnam],''), ' ', isnull([ylsloc],''))) as [Address]
                 ,[eDMS].[udfCleanupString]([ylloc]) as [Area]
				 ,[eDMS].[udfFilterSlug]([ylloc]) as [AreaSlug]
                 ,[eDMS].[udfCleanupString]([yarsyn]) as [AreaSynonyms]
				 ,[eDMS].[udfCleanupString]([ccty]) as [City]
				 ,[eDMS].[udfFilterSlug]([ccty]) as [CitySlug]
				 ,[eDMS].[udfCleanupString](i.[Synonyms]) as [CitySynonyms]
				 ,[eDMS].[udfCleanupString]([cpin]) as [PinCode]
                 ,[eDMS].[udfCleanupString]([ypoarsrv]) as [ZonesServed]
                 ,[eDMS].[udfCleanupString]([czone]) as [Zone]
                 ,[eDMS].[udfCleanupString]([cstate]) as [State]
                 ,[eDMS].[udfCleanupString]([ylcry]) as [Country]
                 ,[lat] as [Latitude]
                 ,[lon] as [Longitude]
				 -- company
                 ,[eDMS].[udfCleanupString]([yconam]) as [CompanyName]
                 ,[eDMS].[udfCleanupString](g.[CompanySynonym]) as [CompanyAliases]
                 ,[eDMS].[udfCleanupString]([ycodec]) as [CompanyDescription]
				 ,[eDMS].[udfCleanupString]([ypsupkey]) as [CompanyKeywords]
				 ,[eDMS].[udfCleanupString]([cimglogo1]) as [CompanyLogo]
				 ,[eDMS].[udfCleanupString](h.[website]) as [AdvertiserURL]
                 ,[eDMS].[udfCleanupString]([ylptcnam]) as [PrimaryContactName]
                 ,[eDMS].[udfCleanupString]([ylptcmob]) as [PrimaryContactMobile]
				 ,[eDMS].[udfCleanupString]([ylptcem]) as [PrimaryContactEmail]
                 ,[eDMS].[udfCleanupString]([ylmob1]) as [LocationMobile]
				 ,[eDMS].[udfCleanupString]([ylph1]) as [LocationLandLine]
				 ,[eDMS].[udfCleanupString]([ylem]) as [LocationEmail]
				 ,[eDMS].[udfCleanupString]([strGen1]) as [LocationDIDNumber]
				 -- category
                 ,[eDMS].[udfCleanupString]([ypcat1]) as [L1Category]
                 ,[eDMS].[udfCleanupString]([ypcat2]) as [L2Category]
                 ,[eDMS].[udfCleanupString]([ypcat3]) as [L3Category]
				 ,[eDMS].[udfFilterSlug]([ypcat3]) as [L3CategorySlug]
				 ,[eDMS].[udfCleanupString]([cmnscat]) as [CategoryPath]
				 ,[eDMS].[udfCleanupString]([ypcat3keys]) as [CategoryKeywords]
				 ,[eDMS].[udfCleanupString]([ypcat3a1]) as [A1]
                 ,[eDMS].[udfCleanupString]([ypcat3a2]) as [A2]
                 ,[eDMS].[udfCleanupString]([ypcat3atr3]) as [A3]
                 ,[eDMS].[udfCleanupString]([ypcat3a4]) as [A4]
                 ,[eDMS].[udfCleanupString]([ypcat3a5]) as [A5]
                 ,[eDMS].[udfCleanupString]([ypcat3a6]) as [A6]
                 ,[eDMS].[udfCleanupString]([ypcat3a7]) as [A7]
                 ,[eDMS].[udfCleanupString]([ypcat3a8]) as [A8]
                 ,[eDMS].[udfCleanupString]([ypcat3a9]) as [A9]
                 ,[eDMS].[udfCleanupString]([ypcat3a10]) as [A10]
                 ,[eDMS].[udfCleanupString]([ypcat3a11]) as [A11]
                 ,[eDMS].[udfCleanupString]([ypcat3a12]) as [A12]
                 ,[eDMS].[udfCleanupString]([ypcat3a13]) as [A13]
                 ,[eDMS].[udfCleanupString]([ypcat3a14]) as [A14]
                 ,[eDMS].[udfCleanupString]([ypcat3a15]) as [A15]
                 ,[eDMS].[udfCleanupString]([ypcat3a16]) as [A16]
                 ,[eDMS].[udfCleanupString]([ypcat3a17]) as [A17]
                 ,[eDMS].[udfCleanupString]([ypcat3a18]) as [A18]
                 ,[ypcat3a19] as [A19]
                 ,[ypcat3a20] as [A20]
				 ,d.Label as [Label]
				 ,d.Col_id as [Col_id]
                 -- product ???
				 ,[eDMS].[udfCleanupString]([ypnam]) as [ProductName]
				 ,[eDMS].[udfCleanupString]([ypdesc]) as [ProductDescription]
				 ,[eDMS].[udfCleanupString]([ypbrdnam]) as [ProductBrand]
				 ,[eDMS].[udfCleanupString]([cimg1]) as [ProductImages]
             from [FastSynchronization].[eDMS].[GetitFlatfile_Static] a (nolock)
       inner join [FastSynchronization].[eDMS].[BizLocSearchID_Staging] ids (nolock)
               on ids.BusinessUserID = a.yusrid
              and ids.LocationID = a.intgen1
  left outer join [10.0.4.43].[GetitOnline].[dbo].[category3] b (nolock)
               on a.[ypcat3id] = b.[cat3_id]
  left outer join [10.0.4.43].[GetitOnline].[dbo].[AdditionalInfoTemplate] c (nolock)
               on b.[Additionalinfotemplate_id] = c.AdditionalInfoTemplate_id_new
  left outer join [10.0.4.43].[GetitOnline].[dbo].[TemplateFields] d (nolock)
               on c.AdditionalInfoTemplate_id = d.AdditionalInfoTemplate_id
  left outer join [10.0.4.43].[GetitOnline].[dbo].[BusinessInfo] e (nolock)
               on a.[batvycoid] = e.[business_id]
  left outer join [10.0.4.43].[GetitOnline].[dbo].[users] f (nolock)
               on e.[user_id] = f.[user_id]
  left outer join [10.0.4.25].[EDMS].[dbo].[DMSMaster] g (nolock)
               on f.[Master_id] = g.[Master_Id]
  left outer join [10.0.4.43].[GetitOnline].[dbo].[companylocation] h (nolock)
               on a.[intgen1] = h.[id]
			  and a.[yusrid] = h.[user_id]
  left outer join [10.0.4.25].[EDMS].[dbo].[City] i (nolock)
               on a.[ccty] = i.[city]
			  and coalesce(i.[Synonyms],'') <> ''
 ) inn
            pivot(max(inn.Label) for inn.Col_id IN ( [1],[2],[3],[4],[5],[6],[7],[8],[9],[10],[11],[12],[13],[14],[15],[16],[17],[18],[19],[20])
 ) as p
 declare @StagedRows bigint = @@ROWCOUNT
 select @StagedRows as [BizLocSearch_Staging]

-- bulk-write fetched rows to zipped text files, calculate md5, move to destination folder

SET @FromRowID = 1
SET @ToBookmarkDT = case when @TotalRows > 0 then @NextBookmarkDT else @FromBookmarkDT end

declare @FileName varchar(max)

WHILE @FromRowID <= @TotalRows
BEGIN
  SET @ToRowID = @FromRowID + @BatchSize
  SET @query = '"select * from [FastSynchronization].[eDMS].[BizLocSearch_Staging] where [RowID] >= '+cast(@FromRowID as varchar(20))+' and [RowID] < '+cast(@ToRowID as varchar(20))+' order by [RowID] asc"'
  SET @FileName = @FilePrefix
    + cast('.' as varchar(max))
	+ replace(convert(varchar(max), @FromBookmarkDT, 120), ':','-')
	+ cast('_' as varchar(max))
	+ replace(convert(varchar(max), @ToBookmarkDT, 120), ':','-')
	+ cast('.' as varchar(max))
	+ cast(@FromRowID as varchar(max))
	+ cast('-' as varchar(max))
	+ cast((@ToRowID-1) as varchar(max))
  
  declare @cmd varchar(7000)
    = cast('bcp ' as varchar(max))
	+ @query
	+ cast(' queryout "' as varchar(max))
	+ @BasePath
	+ cast('\' as varchar(max))
    + @FileName
	+ cast('.txt" -b ' as varchar(max))
	+ cast((case when @BatchSize/10 > 1 then @BatchSize/10 else 1 end) as varchar(max))
	+ cast(' -c -T -r \r -a 65534 -U Aditya_usr -P @ditya -S GIMAZSQLTST002\GIMAZSQLTST001' as varchar(max))
  
  select @cmd
  EXEC master..XP_CMDSHELL @cmd
  
  SET @cmd = 
      cast('C:\"Program Files"\Dos2Unix\bin\dos2unix.exe --force "' as varchar(max))
	+ @BasePath
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.txt"' as varchar(max))
  select @cmd
  EXEC master..XP_CMDSHELL @cmd


  SET @cmd = 
      cast('C:\"Program Files"\UnxUtils\usr\local\wbin\gzip.exe "' as varchar(max))
	+ @BasePath 
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.txt"' as varchar(max))
  select @cmd
  EXEC master..XP_CMDSHELL @cmd
 
  SET @cmd = 
      cast('C:\"Program Files"\UnxUtils\usr\local\wbin\md5sum.exe "' as varchar(max))
	+ @BasePath
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.txt.gz" > "' as varchar(max)) 
	+ @BasePath 
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.md5"' as varchar(max))
  select @cmd
  EXEC master..XP_CMDSHELL @cmd

  SET @cmd = 
      cast('move "' as varchar(max))
	+ @BasePath
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.txt.gz" "' as varchar(max)) 
	+ @DestPath 
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.txt.gz"' as varchar(max))
  select @cmd
  EXEC master..XP_CMDSHELL @cmd

  SET @cmd = 
      cast('move "' as varchar(max))
	+ @BasePath
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.md5" "' as varchar(max)) 
	+ @DestPath 
	+ cast('\' as varchar(max))
	+ @FileName
	+ cast('.md5"' as varchar(max))
  select @cmd
  EXEC master..XP_CMDSHELL @cmd

  

  SET @FromRowID = @ToRowID
END

-- bump bookmark on success

insert into [FastSynchronization].[eDMS].[BizLocSearch_Bookmark] (
  [BookmarkDT]
 ,[JobStartDT]
 ,[JobEndDT]
 ,[RowCount]
) values (
  @ToBookmarkDT
 ,@JobStartTime
 ,GETDATE()
 ,@StagedRows
)

SELECT top 2 * from [FastSynchronization].[eDMS].[BizLocSearch_Bookmark] order by [ID] desc

END

-- select * from [FastSynchronization].[eDMS].[BizLocSearch_Bookmark]
-- truncate table [FastSynchronization].[eDMS].[BizLocSearch_Bookmark]
