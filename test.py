
import filemanager as fm
def correct_ratiofile(full_path):
    old_text1 = "CompanyNumber;AccountingYear;Schema;RAT001;RAT002;RAT003;RAT004;RAT005;RAT006;RAT007;RAT008;RAT009;" \
               "RAT010;RAT011;RAT012;RAT013;RAT014;RAT015;RAT016;RAT017;RAT018;RAT019;RAT020;RAT021;RAT101;RAT102;" \
               "RAT103;RAT104;RAT105;RAT106;RAT107;RAT108;RAT109;RAT110;RAT111;RAT112;RAT113;RAT114;RAT115;RAT116;" \
               "RAT117;RAT118;RAT119;RAT120;RAT121;RAT125;RAT127;RAT201;RAT202;RAT203;RAT204;RAT205;RAT206;RAT207;" \
               "RAT208;RAT209;RAT210;RAT211;RAT212;RAT213;RAT214;RAT217;RAT218;RAT219;RAT220;RAT221"

    #RAT115 en RAT116 ontbreken (file
    old_text2 = "CompanyNumber;AccountingYear;Schema;RAT001;RAT002;RAT003;RAT004;RAT005;RAT006;RAT007;RAT008;RAT009;" \
                "RAT010;RAT011;RAT012;RAT013;RAT014;RAT015;RAT016;RAT017;RAT018;RAT019;RAT020;RAT021;RAT101;RAT102;" \
                "RAT103;RAT104;RAT105;RAT106;RAT107;RAT108;RAT109;RAT110;RAT111;RAT112;RAT113;RAT114;" \
                "RAT117;RAT118;RAT119;RAT120;RAT121;RAT125;RAT127;RAT201;RAT202;RAT203;RAT204;RAT205;RAT206;RAT207;" \
                "RAT208;RAT209;RAT210;RAT211;RAT212;RAT213;RAT214;RAT217;RAT218;RAT219;RAT220;RAT221"
    path = fm.get_path(full_path) # get path
    file = fm.get_filename(full_path) # get filename
    file_without_ext = fm.get_filename_without_extension(file) # get filename without extension
    # file_new = file_without_ext + "bis.csv" # get new filename
    # full_path_bis = os.path.join(path, file_new) # get full_path new

    with open(full_path) as infile:
        data = infile.read().splitlines()
        if data[0] == old_text1:
            data[0] = data[0] + ";x1;x2;x3"
            # with open(full_path_bis, "w") as outfile:
            with open(full_path, "w") as outfile:
                for line in data:
                    outfile.write(line + "\n")
    # os.remove(full_path)
    # os.rename(full_path_bis, full_path)