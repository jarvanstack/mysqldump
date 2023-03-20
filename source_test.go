package mysqldump

import "testing"

func Test_mergeInsert(t *testing.T) {
	type args struct {
		insertSQLs []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			args: args{
				insertSQLs: []string{
					"INSERT INTO `test` VALUES (1, 'a');",
					"INSERT INTO `test` VALUES (2, 'b');",
				},
			},
			want:    "INSERT INTO `test` VALUES (1, 'a'), (2, 'b');",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := mergeInsert(tt.args.insertSQLs)
			if (err != nil) != tt.wantErr {
				t.Errorf("mergeInsert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("mergeInsert() = %v, want %v", got, tt.want)
			}
		})
	}
}
